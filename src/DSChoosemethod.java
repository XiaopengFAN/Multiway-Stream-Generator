import java.util.Map;

public class DSChoosemethod {
    DSModel[] models;
    double zDouble;
    int zInt;

    DSChoosemethod(DSModel[] models, int zInt){
        this.models = models;
        this.zInt = zInt;
    }

    DSChoosemethod(DSModel[] models, double zDouble){
        this.models = models;
        this.zDouble = zDouble;
    }

    public boolean op(int ... args) {
//        You need to override this method as required
        return ( (args[0]-args[1])>0 && (args[1]-args[2])>0 );
    }

    public boolean op(double ... args) {
//        You need to override this method as required
        return ((args[0]-args[1])>0);
    }

    public double calc() {
        double chooseRate = 0;

//        Traverse all situations by using a M-layer-loop.
//        Since there might be different use cases, you can use pruning if possible.
        for (Map.Entry<Integer,Double> entry1 : models[0].getProbmap().entrySet()){
            int num1 = entry1.getKey();
            double prob1 = entry1.getValue();

            for (Map.Entry<Integer,Double> entry2 : models[1].getProbmap().entrySet()){
                int num2 = entry2.getKey();
                double prob2 = entry2.getValue();

                for (Map.Entry<Integer,Double> entry3 : models[2].getProbmap().entrySet()) {
                    int num3 = entry3.getKey();
                    double prob3 = entry3.getValue();

                    if (op(num1,num2,num3)) {
//                        This fomular means these keys are independent.
                        double p = prob1 * prob2 * prob3;
                        chooseRate += p;
//                        System.out.println(num1+"\t"+num2+"\t"+num3+"\t"+chooseRate);
                    }
                }
            }
        }

//        Print out the choose-rate.
//        System.out.println("Your chooseRate of " + model1.getKey() + " and " + model2.getKey() +
//                " is " + Double.toString(chooseRate));
        return chooseRate;
    }

}
