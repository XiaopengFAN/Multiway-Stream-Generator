import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DSChoosemethod {
//    This Class can be inherit to Override theta_join() and calChooseRate() methods.
//    It's Theta join is keyA > keyB > keyC
    DSModel[] models;
    private double chooseRate=0;

    synchronized private void addProb(double prob){
        chooseRate += prob;
    }

    DSChoosemethod(DSModel[] models){
        this.models = models;
    }

    public boolean theta_join(int ... args) {
//        You need to override this method for your use case
        return ( (args[0]-args[1])>0 && (args[1]-args[2])>0 );
    }

    public boolean theta_join(double ... args) {
//        You need to override this method for your use case
        return ( (args[0]-args[1])>0 && (args[1]-args[2])>0 );
    }

    public static double[] calChooseRate(DSChoosemethod[] chmethods) {
//        It will return the choose-rate of all theta_join;
        double[] rates = new double[chmethods.length];
        int i = 0;
//        Calculate choose-rate of each method
        for (DSChoosemethod chmethod : chmethods) {
            rates[i++] = chmethod.calc();
        }
        return rates;
    }

        public double calc() {
        double chooseRate = 0;

//        Traverse all situations by using a M-layer-lotheta_join.
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

                    if (theta_join(num1,num2,num3)) {
//                        This fomular means these keys are independent.
                        double p = prob1 * prob2 * prob3;
                        chooseRate += p;
//                        System.out.println(num1+"\t"+num2+"\t"+num3+"\t"+chooseRate);
                    }
                }
            }
        }

//        Print out the choose-rate.
//        System.out.println("Your chooseRate of " + models[0].getKey() + ", " + models[1].getKey()
//                + " and " + models[2].getKey() +
//                " is " + Double.toString(chooseRate));
        return chooseRate;
    }

}
