```@meta
CurrentModule = TypeDBClient
```

# Contributing

## Introduction

So, TypeDB itself provides various ways how to communicate (read from & write to) with the database.

There is the [Workbase] (https://dev.typedb.ai/docs/workbase/overview) the [TypeDB Console] (https://dev.typedb.ai/docs/running-typedb/console) and the different [Clients] (https://dev.typedb.ai/docs/client-api/overview) to be used in an application. 

There are clients for the following languages: Java, Node.js and Python. But there is no Julia client yet.

We are going to build the Julia client here.

## Workflow guidance & roadmap

```@index
```

Our reference client is the [Python version](https://dev.typedb.ai/docs/client-api/python), which can be found also on [PyPi](https://pypi.org/project/typedb-client/#history).

However, there are some design guidelines on how to roll a client [here](https://dev.typedb.ai/docs/client-api/python) and an introductory blog on how to start [here](https://blog.typedb.ai/typedb-python-driver-how-to-roll-your-own-b010bbd73023).

The introductory blog is for TypeDB 1.x iteration, so it can only serve as a general introduction. Requirements for TypeDB 2.0 are likely a bit different.

The roadmap for TypeDBClient.jl (24.01.2021):

- writing a HTTP/2 and gRPC implementation in Julia (WIP)
- building the client architecture in Julia based on the protocol
- building up BDD infrastructure  

The roadmap for TypeDB 2.0 (24.01.2021):

- all client libraries have been released
- TypeDB 2.0 production release expected in first quarter of 2021  

## Dependencies

We aim for a Julia native solution, but until then we need to call into Python.

- [PyCall](https://github.com/JuliaPy/PyCall.jl)
- [Protobuf](https://github.com/protocolbuffers/protobuf)
- [gRPC](https://grpc.io/)

## How to start

Check for open issues [here](https://github.com/Humans-of-Julia/TypeDBClient.jl/issues).

For further questions about how to start, ideally find us on [Discord](https://discord.gg/NSYrYZQRyv).

## Project status

The current project status can be seen on the [GitHub Project Kanban](https://github.com/Humans-of-Julia/TypeDBClient.jl/projects/1)

## Useful sources

- [How I translate Python code into Julia code](https://stackoverflow.com/questions/59356818/how-i-translate-python-code-into-julia-code)
- [Python to Julia Quick translation / conversion reference Guide](https://gist.github.com/cuckookernel/9777067)
- [Syntax Cheatsheet reference](https://cheatsheets.quantecon.org/)
- [Client-Python tests](https://github.com/Humans-of-Julia/TypeDBClient.jl/tree/dev/client-python-reference/tests/integration)
- [Protobuf](https://github.com/protocolbuffers/protobuf)
- [gRPC](https://grpc.io/)
- [gRPC implementation introduction](https://scotch.io/tutorials/implementing-remote-procedure-calls-with-grpc-and-protocol-buffers)

```@autodocs
Modules = [TypeDBClient]
```
