For datacenter applications that require tight synchronization, transactions are
commonly employed for achieving concurrency while preserving correctness.
Unfortunately, distributed transactions are hard to scale due to the
decentralized lock acquisition and coordination protocols they employ.
We investigate the use of a centralized lock broker architecture to improve the
efficiency/scalability for distributed transactions, and present the design and
development of such a framework, called {\sc Panopticon}.


Panopticon achieves efficiency/scalability by divorcing locks from the data
items and migrating locks to improve lock access locality.
More specifically, the lock broker mediates the access to data shared across servers
by migrating the associated locks like tokens, and in the process learns and improves
the access locality of transactions.


Our experiments show that Panopticon performs better than distributed
transactions as the number of data items and number of servers involved in
transactions increase. Moreover, as the history locality (the probability of
using the same objects in consecutive transactions) increase, Panopticon's lock
migration strategies improve lock-access locality and result in significantly
better performance. Finally, we also show that, by employing simple learning
techniques, the broker can further improve the lock access locality and, hence,
the performance of distributed transactions.
