# Outperforming nginx at static file serving

Rough architecture:

- Thread per core. Every connection is bound to a thread.
- Every IO operation (opening files, reading files, sending data to a socket) is done via
    io_uring operations. When operations do not depend on previous results, they are submitted
    together and possibly linked in order via the IO_LINK flag
- Each thread allocates a big chunk of contiguous memory at the start of the program.
  Afterwards, each request gets a part of that memory to allocate into.
  The memory is probably registered with the kernel via io_uring register_buffers (TODO)
- HTTP2 is supported and connections are kept around for re-use.
      
