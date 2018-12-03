SummaryStore is an approximate time-series data store, designed for
analytics applications, capable of storing large volumes of data (tested
up to 1 petabyte) on a single node.

SummaryStore uses a novel _time-decayed summarization_ mechanism to
significantly compact data while maintaining high accuracy in many
applications.  For details see our paper at [SOSP'17](SummaryStore/papers/17sosp.pdf).

## COMPILING

```bash
cd SummaryStore
mvn package
```


## USAGE

SummaryStore is meant to be used as an embedded data store. After
compiling, link against the JARs in SummaryStore/store/target, and create
an instance of the SummaryStore class.


## PUBLICATIONS

* **Low-latency analytics on colossal data streams with SummaryStore**
 [[pdf](SummaryStore/papers/17sosp.pdf)]
 [[bib](SummaryStore/papers/17sosp.bib)]  
  Nitin Agrawal, Ashish Vulimiri  
  _26th Symposium on Operating Systems Principles (SOSP'17), October 2017_

* **Learning with less: Can approximate storage systems save learning from drowning in data?**
 [[pdf](SummaryStore/papers/17aisys.pdf)]  
  Nitin Agrawal, Ashish Vulimiri  
  _Workshop on AI Systems at Symposium on Operating Systems Principles (SOSP), October 2017_

* **Building highly-available geo-distributed data stores for continuous learning**
 [[pdf](SummaryStore/papers/18mlsys.pdf)]  
  Nitin Agrawal, Ashish Vulimiri  
  _Workshop on Systems for ML at NIPS '18, December 2018_



## CONTACT

Nitin Agrawal (nitina.a@gmail.com)

Ashish Vulimiri (ashish@vulimiri.net)
