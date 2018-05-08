public class Test {
    public static void main(String args[]){
        DSModel[] models = new DSModel[4];
//        Choose your model, and set parameters supposed
//        If you  want some user-defined model, then pass your value-set and probabilities-set.
//        If the total CDF is not 1, the algorithm will correct it automaticlly.
        models[0] = new DSModel();
        int[] kArray = {1,2,3};
        double[] probability = {1.0/3.0, 1.0/3.0, 1.0/3.0};
        models[0].useUserDefineInt("BusID",kArray, probability);
//        You can choose the origin model by using model.useXXXMODEL(ELEMENT_TYPE parameters).
        models[1] = new DSModel();
        models[1].useUniformInt("AA",0,10);
        models[2] = new DSModel();
        models[2].useUniformInt("BB",0,10);
        models[3] = new DSModel();
        models[3].useUniformInt("CC",0,10);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
//        It's NOT FINISHED yet.
        DSChoosemethod[] chmethods = new DSChoosemethod[1];
        DSModel[] chKey0 = new DSModel[3];
        for (int i = 0; i < 3; i++) {
            chKey0[i] = models[i+1];
        }
        chmethods[0] = new DSChoosemethod(chKey0,0);

//        Then send all your models into DSTuple, it will generate tuples for you.
//        You can change I/O format in it.
        DSTuple tuple = new DSTuple(models,chmethods);
        tuple.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.

//        Finally set threads' period and amount that you want.
//        Then pass your DSTuple and DSChoosemethod, start produce tuples！
        MultiwayStreamGenerator ds = new MultiwayStreamGenerator(10,3, tuple);
        ds.start();
    }
}
