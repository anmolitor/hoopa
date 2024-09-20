# Outperforming nginx at static file serving

Rough architecture:
```
       _________________
      |                 |   AcceptMulti     __________           _____________
      |                 | ---------------> |          |         |             |        
      |   Main thread   |                  | IO_URING | <-----> |             |                 
      |                 | <--------------  |__________|         |             |        
      |_________________|    Connection         ^   |           |      L      |      
                     |                          |   |           |      I      |      
                     |__________________________|   |           |      N      |         
                     send connection to other ring  |           |      U      |      
                                                    |           |      X      |      
                      ______________________________|           |             |      
                     |  receive accepted connection             |      K      |    
       ______________V__                                        |      E      |     
      |                 |    RecvMulti      __________          |      R      |                
      |                 | ---------------> |          |         |      N      |        
      |                 | <--------------- |          |         |      E      |        
      |                 |   HTTP Request   |          |         |      L      |                                    
      |                 | OpenFile & Statx |          |         |             |        
      |  Worker thread  | ---------------> | IO_URING | <-----> |             |        
      |                 | <--------------- |          |         |             |        
      |                 |  File Descriptor |          |         |             |        
      |               ReadFixed & Splice & Send       |         |             |        
      |                 | ---------------> |          |         |             |        
      |_________________| <--------------- |__________|         |_____________|                 
                              Success!
```
      
          

- Thread per core. Every connection is bound to a thread.
- Every IO operation (opening files, reading files, sending data to a socket) is done via
    io_uring operations. When operations do not depend on previous results, they are submitted
    together and possibly linked in order via the IO_LINK flag
- Each thread allocates a big chunk of contiguous memory at the start of the program.
  Afterwards, each request gets a part of that memory to allocate into.
  The memory is registered with the kernel via io_uring register_buf_ring
- HTTP2 is supported and connections are kept around for re-use.


      
