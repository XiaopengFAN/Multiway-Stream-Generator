## Multiway-Stream-Generator

Multiway Stream Generator (MS-Generator) can generate multi-way streams for researchers to evaluate the performance of their algorithms or application.

## Functions

This version currently implements the following functions:
```markdown
1. set a tuple's keys.

2. for each key, you can choose according to the probability distribution model of random data.
The current distribution models are: gaussian distribution, exponential distribution, poisson distribution, Zipf distribution and uniformal distribution.

3. set frequency, maximum concurrency of the generator.

4. set the tuple's primary key, and their proportional distribution of different values (e.g. product A 60%, product B 40%).

5. calculate the selectivity for each particular theta join.
```

## QuikUse

In order to get a generator, you need to create a MultiwayStreamGenerator.
In order to get a MultiwayStreamGenerator, you need to create DSModels and DSJoin.

### Create DSModel

All properties and their probability distributions in a tuple are stored using a model instance array DSModel[].

For example, our tuple has NAME and AGE two keys.

For the NAME, we hope that the range of the generated data is {1,2,3}, and the proportion is {1/3,1/3,1/3};

For AGE, we want it to generate data by the poisson distribution of lambda=10.

The method to call the interface is:

```markdown
        DSModel[] models = new DSModel[2];
//        Choose your model, and set parameters supposed
//        If you  want some user-defined model, then pass your value-set and probabilities-set.
        models[0] = new DSModel();
        int[] kArray = {1,2,3};
        double[] probability = {0.33,0.33,0.33};  // If the total CDF is not 1, the algorithm will correct it.
        models[0].useUserDefine("NAME",kArray, probability);
//        You can choose the origin model by using model.useXXXMODEL(ELEMENT_TYPE parameters).
        models[1] = new DSModel();
        models[1].usePossion("AGE",10);

```

### Create DSJoin

This Class can be inherit to Override op() and calc() methods.
It's Theta join is keyA > keyB > keyC
```markdown

        DSJoin[] joins = new DSJoin[1];
        DSModel[] chKey0 = new DSModel[3];           // cheKey_i is the DSModel you want to join in joins[i] ,
        for (int i = 0; i < chKey0.length; i++) {
            chKey0[i] = models[i+1];
        }
        joins[0] = new DSJoin(chKey0,0);
```

### Create DSTuple

When you've set all models and choosemethods, you can pass it to a tuple.
```markdown
//        Then send all your models into DSTuple, it will generate tuples for you.
//        You can change I/O format in it.
        DSTuple tuple = new DSTuple(models);
```

### Create MultiwayStreamGenerator

Now you can pass your models and choosemethods to a generator. And if you want a period of 10ms, 3 threads, then you can set parameters as:
```markdown
//        Finally set threads' period and amount that you want.
//        Then pass your DSTuple and DSJoin, start produce tuples！
        UnboundedDataSource ds = new UnboundedDataSource(10,3, tuple, chmethd);
        ds.start();

```

### More Generator

If you need different generator to work together, just create more MultiwayStreamGenerator.
