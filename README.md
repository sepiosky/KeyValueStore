# Key-Value Database
Implementation of Fault-Tolerant Key-Value Store

this is final programming assignment of [Cloud Computing Concepts, Part 2 (Coursera)](https://www.coursera.org/learn/cloud-computing-2?specialization=cloud-computing) (for full specifications of project you can read `mp2_specifications.pdf`)

This system provides following functionalities:
- A key-value store supporting CRUD operations (Create, Read, Update, Delete).
- Load-balancing (via a consistent hashing ring to hash both servers and keys).
- Fault-tolerance up to two failures (by replicating each key three times to three successive nodes
  in the ring, starting from the first node at or to the clockwise of the hashed key).
- Quorum consistency level for both reads and writes (at least two rep
- Stabilization after failure (recreate three replicas after failure).

## How to run
you can implement your Application layer.
the CRUD API's for client side are provided by `clientCreate`, `clientRead`, `clientUpdate` and `clientDelete` functions implemented in `MP2Node` files.
this system uses [Gossip Membership Protocol](https://github.com/sepiosky/GossipMembershipProtocol) implementation from final assignment of course's part 1.

you can run tests provided in `/testcases/*.conf`:
```
make
./Aplication ./testcases/msgdropsinglefailure.conf
```

You can run all testcases using `run.sh` and check results in `.log` files.
```
./run.sh
```
