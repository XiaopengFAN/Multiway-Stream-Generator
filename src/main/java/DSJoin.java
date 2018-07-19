import java.util.Map;

public class DSJoin {
//    This Class can be inherit to Override theta_join() and calChooseRate() methods.
//    It's Theta join is keyA > keyB > keyC
    DSModel[] models;
    private double selectivity=0;

    synchronized private void addProb(double prob){
        selectivity += prob;
    }

    DSJoin(DSModel[] models){
        this.models = models;
    }

    public boolean theta_join(int ... args) {
//        You need to override this method for your use case
        return ( (args[0]-args[1])>0 && (args[1]-args[2])>0 && (args[2]-args[3])>0
                && (args[3]-args[4])>0 );
    }

    public boolean theta_join(double ... args) {
//        You need to override this method for your use case
        return ( (args[0]-args[1])>0 && (args[1]-args[2])>0 && (args[2]-args[3])>0
                && (args[3]-args[4])>0 );
    }

    public static double[] calChooseRate(DSJoin[] joins) {
//        It will return the choose-rate of all theta_join;
        double[] rates = new double[joins.length];
        int i = 0;
//        Calculate choose-rate of each method
        for (DSJoin join_method : joins) {
            rates[i++] = join_method.calc();
        }
        return rates;
    }

    private double calc() {
        double selectivity = 0;

//        Traverse all situations by using a M-layer-lotheta_join.
//        Since there might be different use cases, you can use pruning if possible.
        if (models[0].getMd() == "UniformInt"){
            selectivity = generalCalc();
        } else if(models[0].getMd() == "Poisson") {
            selectivity = generalCalc();
        } else{
            selectivity = generalCalc();
        }

//        Print out the choose-rate.
//        System.out.println("Your selectivity of " + models[0].getKey() + ", " + models[1].getKey()
//                + " and " + models[2].getKey() +
//                " is " + Double.toString(selectivity));
        return selectivity;
    }

    private double generalCalc(){
        double selectivity = 0;

//        Traverse all situations by using a M-layer-lotheta_join.
//        Since there might be different use cases, you can use pruning if possible.
        for (Map.Entry<Integer,Double> entry1 : models[0].getProbmap().entrySet()){
            int num1 = entry1.getKey();
            double prob1 = entry1.getValue();

            for (Map.Entry<Integer,Double> entry2 : models[1].getProbmap().entrySet()){
                int num2 = entry2.getKey();
                double prob2 = entry2.getValue();
                if (num1<num2)
                    break;

                for (Map.Entry<Integer,Double> entry3 : models[2].getProbmap().entrySet()) {
                    int num3 = entry3.getKey();
                    double prob3 = entry3.getValue();
                    if (num2<num3)
                        break;

                    for (Map.Entry<Integer,Double> entry4 : models[3].getProbmap().entrySet()) {
                        int num4 = entry4.getKey();
                        double prob4 = entry4.getValue();
                        if (num3 < num4)
                            break;

                        for (Map.Entry<Integer, Double> entry5 : models[4].getProbmap().entrySet()) {
                            int num5 = entry5.getKey();
                            double prob5 = entry5.getValue();
                            if (num4 < num5)
                                break;

//                                System.out.println(num1+"\t"+num2+"\t"+num3);
                            if (theta_join(num1, num2, num3, num4, num5)) {
//                                This fomular means these keys are independent.
                                double p = prob1 * prob2 * prob3 * prob4 * prob5;
                                selectivity += p;
//                                System.out.println(num1+"\t"+num2+"\t"+num3+"\t"+selectivity);
                            }
                        }
                    }
                }
            }
        }
        return selectivity;
    }

}
