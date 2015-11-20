// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// Crimson: a prototype high performance OSD

// Copyright (C) 2015 Casey Bodley <cbodley@redhat.com>
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1 of
// the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
// 02110-1301 USA

extern "C" {
#include <fio.h>
}

#include <boost/program_options.hpp>
#include <capnp/message.h>
#include <crimson/direct_messenger.h>
#include <crimson/fio-engine.h>
#include <crimson/osd.h>

namespace bpo = boost::program_options;

using namespace crimson;
using crimson::net::Connection;
using crimson::net::DirectConnection;

namespace {

class OsdBackend : public fio::Backend {
  // encapsulate state inside of a separate class, because it has to be
  // allocated/freed on init()/shutdown()
  class Impl {
    osd::OSD osd;
    shared_ptr<Connection> client;
    std::map<uint32_t, promise<>> promises;
    uint32_t sequence{0};

    future<> handle_reply(Connection::MessageReaderPtr&& reader);

   public:
    Impl();
    ~Impl();

    future<> handle(io_u* unit);
    future<> shutdown();
  };
  std::unique_ptr<Impl> impl;

 public:
  future<> init() override {
    impl = std::make_unique<Impl>();
    return now();
  }

  future<> handle_request(io_u* unit) override {
    return impl->handle(unit).then_wrapped(
      [unit] (auto f) {
        // translate exceptions into fio errors
        try {
          f.get();
          unit->error = 0;
        } catch (std::system_error& e) {
          unit->error = e.code().value();
        } catch (...) {
          unit->error = EIO;
        }
      });
  }

  future<> shutdown() {
    auto fut = impl->shutdown();
    return fut.then([impl = std::move(impl)] {});
  }
};

OsdBackend::Impl::Impl()
{
  auto p = DirectConnection::make_pair();
  client = p.first;
  auto server = p.second;

  keep_doing([this, server] {
      return server->read_message().then(
        [this, server] (Connection::MessageReaderPtr&& reader) {
          return osd.handle_message(server, std::move(reader));
        });
    }).handle_exception([] (auto eptr) {});

  keep_doing([this] {
      return client->read_message().then(
        [this] (Connection::MessageReaderPtr&& reader) {
          return handle_reply(std::move(reader));
        });
    }).handle_exception([] (auto eptr) {});
}

OsdBackend::Impl::~Impl()
{
  // break any outstanding promises
  for (auto& p : promises) {
    p.second.set_exception(
        std::system_error(ECONNRESET, std::system_category()));
  }
}

future<> OsdBackend::Impl::handle(io_u* unit)
{
  // register the completion
  auto seq = sequence++;
  auto p = promises.emplace(std::piecewise_construct,
                            std::forward_as_tuple(seq),
                            std::forward_as_tuple());
  auto fut = p.first->second.get_future();

  // build the message
  auto builder = std::make_unique<capnp::MallocMessageBuilder>();
  auto message = builder->initRoot<proto::Message>();
  message.initHeader().setSequence(seq);

  if (unit->ddir == DDIR_WRITE) {
    auto request = message.initOsdWrite();
    request.setObject(capnp::Text::Builder{unit->file->file_name});
    request.setOffset(unit->offset);
    auto buffer = static_cast<const capnp::byte*>(unit->xfer_buf);
    request.setData(capnp::Data::Reader{buffer, unit->xfer_buflen});
    request.setFlags(proto::osd::write::ON_COMMIT);
  } else if (unit->ddir == DDIR_READ) {
    auto request = message.initOsdRead();
    request.setObject(capnp::Text::Builder{unit->file->file_name});
    request.setOffset(unit->offset);
    request.setLength(unit->xfer_buflen);
  }

  client->write_message(std::move(builder));
  return fut;
}

future<> OsdBackend::Impl::shutdown()
{
  return client->close();
}

future<> OsdBackend::Impl::handle_reply(Connection::MessageReaderPtr&& reader)
{
  auto message = reader->getRoot<proto::Message>();
  // find the associated completion
  auto seq = message.getHeader().getSequence();
  auto p = promises.find(seq);
  if (p == promises.end())
    return now();

  auto completion = std::move(p->second);
  promises.erase(p);

  // check the reply for errors
  uint32_t result = EINVAL;
  if (message.isOsdWriteReply()) {
    auto reply = message.getOsdWriteReply();
    result = reply.isErrorCode() ? reply.getErrorCode() : 0;
  } else if (message.isOsdReadReply()) {
    result = message.getOsdReadReply().getErrorCode();
  }

  // signal the success or failure
  if (result == 0)
    completion.set_value();
  else
    completion.set_exception(
        std::system_error(result, std::system_category()));
  return now();
}

struct OsdEngine {
  OsdBackend backend;
  fio::Engine engine;

  OsdEngine(const bpo::variables_map& cfg, uint32_t iodepth)
    : engine(cfg, &backend, iodepth) {}
};

template <typename T, size_t N>
constexpr size_t arraysize(const T(&)[N]) { return N; }

int fio_crimson_init(thread_data* td)
{
  const char* argv[] = {
    "crimson-osd",
    "-c", "1", // don't spawn any extra seastar threads
    "-m", "1M", // don't hog all of the memory
    // seastar segfaults on SIGINT because it comes from a non-seastar thread
    "--no-handle-interrupt",
  };
  auto argc = arraysize(argv);

  // seastar can only have one polling thread, so we can't support multiple jobs
  if (td->thread_number > 1) {
    std::cerr << argv[0] << ": multiple jobs are not supported" << std::endl;
    return -1;
  }

  auto options = fio::Engine::get_options_description();
  bpo::variables_map cfg;
  cfg.emplace("argv0", bpo::variable_value(std::string{argv[0]}, false));

  try {
    bpo::store(bpo::command_line_parser(argc, const_cast<char**>(argv))
               .options(options)
               .run(),
               cfg);
  } catch (bpo::error& e) {
    std::cerr << "configuration failed. " << e.what() << std::endl;
    return -1;
  }
  bpo::notify(cfg);

  try {
    td->io_ops->data = new OsdEngine(cfg, td->o.iodepth);
  } catch (std::exception& e) {
    std::cerr << "initialization failed. " << e.what() << std::endl;
    return -1;
  }
  return 0;
}

int fio_crimson_queue(thread_data* td, io_u* unit)
{
  auto c = static_cast<OsdEngine*>(td->io_ops->data);
  switch (unit->ddir) {
    case DDIR_WRITE:
    case DDIR_READ:
      c->engine.queue(unit);
      return FIO_Q_QUEUED;
    default:
      return FIO_Q_COMPLETED;
  }
}

io_u* fio_crimson_event(thread_data* td, int event)
{
  auto c = static_cast<OsdEngine*>(td->io_ops->data);
  return c->engine.get_event(event);
}

int fio_crimson_getevents(thread_data* td, unsigned int min,
                          unsigned int max, const timespec* t)
{
  auto c = static_cast<OsdEngine*>(td->io_ops->data);

  auto timeout = fio::Engine::OptionalTimePoint{};
  if (t) {
    using namespace std::chrono;
    auto duration = seconds{t->tv_sec} + nanoseconds{t->tv_nsec};

    using Clock = fio::Engine::Clock;
    timeout = Clock::now() + duration_cast<Clock::duration>(duration);
  }

  return c->engine.get_events(min, max, timeout);
}

void fio_crimson_cleanup(thread_data* td)
{
  auto c = static_cast<OsdEngine*>(td->io_ops->data);
  td->io_ops->data = nullptr;
  delete c;
}

int fio_crimson_open_file(thread_data* td, fio_file* f)
{
  return 0;
}

int fio_crimson_close_file(thread_data fio_unused* td, fio_file* f)
{
  return 0;
}

struct crimson_ioengine_ops : public ioengine_ops {
  crimson_ioengine_ops() : ioengine_ops({}) {
    strncpy(name, "crimson-osd", sizeof(name));
    version    = FIO_IOOPS_VERSION;
    flags      = FIO_DISKLESSIO;
    init       = fio_crimson_init;
    queue      = fio_crimson_queue;
    getevents  = fio_crimson_getevents;
    event      = fio_crimson_event;
    cleanup    = fio_crimson_cleanup;
    open_file  = fio_crimson_open_file;
    close_file = fio_crimson_close_file;

    register_ioengine(this);
  }

  ~crimson_ioengine_ops() {
    unregister_ioengine(this);
  }
};
static crimson_ioengine_ops ioengine;

} // anonymous namespace
