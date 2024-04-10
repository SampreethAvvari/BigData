# Part 5: Item co-occurrence

## Explain each step of your solution

- **Mapper**: Transforms CSV data into baskets.
- **1st Reducer**: Filters out single product entries and duplicates in baskets, then generates product combinations for each basket.
- **2nd Reducer**: Calculates the count of all combinations for each product.
- **3rd Reducer**: Determines the best combination for each product.

## What problems, if any, did you encounter?

We, the three of us in the group, were on a call and drew out the architecture before diving into the coding. This exercise helped us visualize the necessary number of mappers and reducers, strategize to minimize mapper usage, and understand the outputs at each phase. Once we had a clear picture of how the input CSV file should flow through the mappers and reducers, we began coding. This preparation meant we encountered few hiccups along the way.

## How does your solution scale?

Analyze the time and space used by each stage of your solution, including the number of (intermediate) output values.

**Stage 1/3:** Map tasks took 1,558,122 ms, and reduce tasks took 195,358 ms, highlighting more time spent in mapping. Heap usage was ~245 GB with 362,546 map output bytes and 422,990 materialized bytes. Map outputs were 10,200 records, reduced to 5,686, showing data consolidation.
**Stage 2/3:** Mapping took 1,560,736 ms; reducing took 196,029 ms, maintaining the pattern of map-intensive work. Heap usage rose to ~275 GB. Map outputs were 176,176 bytes, materialized to 232,668 bytes, with 5,686 records reduced to 3,316.
**Stage 3/3:** Map tasks lasted 1,532,755 ms, and reduce tasks 196,872 ms. Heap usage was slightly lower at ~274 GB. Map outputs dropped to 103,260 bytes, materialized at 154,730 bytes, with 3,316 records narrowed down to 158, indicating thorough data reduction.

**Scalability:** The job shows scalability in handling large datasets by distributing the computation across many tasks and nodes. The gradual decrease in the number of records through the stages indicates effective data reduction and consolidation.
