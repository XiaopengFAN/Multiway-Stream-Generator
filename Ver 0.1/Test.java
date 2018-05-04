public class Test {
    public static void main(String args[]){
        DSModel[] models = new DSModel[2];
//        Choose your model, and set parameters supposed
//        If you  want some user-defined model, then pass your value-set and probabilities-set.
        models[0] = new DSModel();
        int[] kArray = {1,2,3};
        double[] probability = {0.33,0.33,0.33};  // If the total CDF is not 1, the algorithm will correct it.
        models[0].useUserDefine("Key",kArray, probability);
//        You can choose the origin model by using model.useXXXMODEL(ELEMENT_TYPE parameters).
        models[1] = new DSModel();
        models[1].usePossion("Value",10);

//        Then send all your models into DSTuple, it will generate tuples for you.
//        You can change I/O format in it.
        DSTuple tuple = new DSTuple(models);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
//        It's NOT FINISHED yet.
        DSChoosemethod chmethd = new DSChoosemethod();

//        Finally set threads' period and amount that you want.
//        Then pass your DSTuple and DSChoosemethod, start produce tuples！
        UnboundedDataSource ds = new UnboundedDataSource(10,3, tuple, chmethd);
        ds.start();
    }
}
