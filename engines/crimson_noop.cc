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
#include <crimson/fio-engine.h>

namespace bpo = boost::program_options;

using namespace crimson;

namespace {

class NoopBackend : public fio::Backend {
 public:
  future<> init() override {
    return now();
  }
  future<> handle_request(io_u* unit) override {
    return now();
  }
  future<> shutdown() override {
    return now();
  }
};

struct NoopEngine {
  NoopBackend backend;
  fio::Engine engine;

  NoopEngine(const bpo::variables_map& cfg, uint32_t iodepth)
    : engine(cfg, &backend, iodepth) {}
};

template <typename T, size_t N>
constexpr size_t arraysize(const T(&)[N]) { return N; }

int fio_crimson_init(thread_data* td)
{
  const char* argv[] = {
    "crimson-noop",
    "-c", "1", // don't spawn any extra seastar threads
    "-m", "1M", // don't hog all of the memory
    // seastar segfaults on SIGINT because it comes from a non-seastar thread
    "--no-handle-interrupt",
  };
  auto argc = arraysize(argv);

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
    td->io_ops->data = new NoopEngine(cfg, td->o.iodepth);
  } catch (std::exception& e) {
    std::cerr << "initialization failed. " << e.what() << std::endl;
    return -1;
  }
  return 0;
}

int fio_crimson_queue(thread_data* td, io_u* unit)
{
  auto c = static_cast<NoopEngine*>(td->io_ops->data);
  c->engine.queue(unit);
  return FIO_Q_QUEUED;
}

io_u* fio_crimson_event(thread_data* td, int event)
{
  auto c = static_cast<NoopEngine*>(td->io_ops->data);
  return c->engine.get_event(event);
}

int fio_crimson_getevents(thread_data* td, unsigned int min,
                          unsigned int max, const timespec* t)
{
  auto c = static_cast<NoopEngine*>(td->io_ops->data);

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
  auto c = static_cast<NoopEngine*>(td->io_ops->data);
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
    static_assert(sizeof(name) > sizeof(char*), "bad sizeof"); // XXX
    strncpy(name, "crimson-noop", sizeof(name));
    version    = FIO_IOOPS_VERSION;
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
