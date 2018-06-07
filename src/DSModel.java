import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class DSModel {
    //    Models
    private Poisson poisson;
    private Zipf zipf;
    private Gaussian gaussian;
    private Exponential exponential;
    private UniformDouble uniformdb;
    private UniformInt uniformint;
    private UserDefineInt userdefineint;

    //    Flags & Memory
    private double sum;
    private String md;                                      // It point out which model is used in this DSModel
    private String key;                                     // The name of this model, or a key in a tuple
    private Map<Integer,Double> map = new HashMap<>();      // It's used as CDF Map
    private Map<Integer,Double> probmap = new HashMap<>();  // It's used as pmf Map

    private boolean haveNextGaussian = false;
    private double nextGaussian = 0;

    //    Test-Memory
    private static Map<Double,Integer> statisticDouble = new TreeMap<>();
    private static Map<Integer,Integer> statisticInt = new TreeMap<>();
    private int count = 0;


    public String getMd() {
        return md;
    }

    public String getKey() {
        return key;
    }

    public Map<Integer,Double> getMap() {
        return map;
    }

    public Map<Integer,Double> getProbmap() {
        return probmap;
    }


    public void useUserDefineInt(String key, int[] kArray, double[] probability){
//        If you  want some user-defined model, then pass your value-set and probabilities-set.
//        If the total CDF is not 1, the algorithm will correct it automaticlly.
        this.md = "UserDefineInt";
        this.key = key;
        this.userdefineint = new UserDefineInt();
        userdefineint.kArray = kArray;
        userdefineint.probability = probability;
        userdefineint.createUserDefineIntMap();
    }

    public void usePoisson(String key, int lambda) {
//        Poisson Distribution
        this.md = "Poisson";
        this.key = key;
        this.poisson = new Poisson();
        poisson.lambda = lambda;
        poisson.createPoissonMap();
    }

    public void useZipf(String key, int N, double s){
//        Zipf Distribution
        this.md = "Zipf";
        this.key = key;
        this.zipf = new Zipf();
        zipf.N = N;
        zipf.s = s;
        zipf.createZipfMap();
    }

    public void useGausian(String key, double mu, double sigma) {
//        Gaussian Distribution
        this.md = "Gaussian";
        this.key = key;
        this.gaussian = new Gaussian();
        gaussian.mu = mu;
        gaussian.sigma = sigma;
    }

    public void useExponential(String key, double lambda) {
//        Exponential Distribution
        this.md = "Exponential";
        this.key = key;
        this.exponential = new Exponential();
        exponential.lambda = lambda;

    }

    public void useUniformDouble(String key, double low, double high) {
//        Uniform Distribution in double
//        ATTENTION, it's range area is [low,high)
        this.md = "UniformDouble";
        this.key = key;
        this.uniformdb = new UniformDouble();
        uniformdb.low = low;
        uniformdb.high = high;
    }

    public void useUniformInt(String key, int low, int high) {
//        Uniform Distribution in integer
//        ATTENTION, it's range area is [low,high)
        this.md = "UniformInt";
        this.key = key;
        this.uniformint = new UniformInt();
        uniformint.low = low;
        uniformint.high = high;
        uniformint.createUniformIntMap();
    }

    public int startRamdomInt() {
//        A switch of integer random
        int randNum = 0;
        if (md == "Poisson") {
            randNum = poisson.generatePoisson();
        } else if (md == "Zipf") {
            randNum = zipf.generateZipf();
        } else if (md == "UniformInt") {
            randNum = uniformint.generateUniformInt();
        } else if (md == "UserDefineInt") {
            randNum = userdefineint.generateUserDefineInt();
        }

//        Done. Nothing to do or you can collect for test.
//        System.out.println(randNum);
//        deal(randNum);

//        Final Part.   Return your result.
        return randNum;
    }

    public double startRamdomDouble() {
//        A switch of double random
        double randNum = 0;
        if (md == "Exponential") {
            randNum = exponential.generateExponential();
        } else if(md == "UniformDouble") {
            randNum = uniformdb.generateUniformDouble();
        } else if(md == "Gaussian"){
            randNum = gaussian.generateGaussian();
        }
//        Done. Nothing to do or you can collect for test.
//        System.out.println(randNum);
//        deal(randNum);

//        Final Part.   Return your result.
        return randNum;
    }

    private class UserDefineInt {
        private int[] kArray;
        private double[] probability;

        private void createUserDefineIntMap() {
            double cdf = 0;
            double check = 0;
            if (kArray.length != probability.length){
                System.out.println("ERROR: kArray and probability's size un-match, please check.");
                System.exit(5);
            }
            for (int i = 0; i < kArray.length; i++) {
                check += probability[i];
            }
            if(check != 1){
//                System.out.println("Warning: The CDF is " + check + ",which not equals to 1. Auto correcting.");
                for (int i = 0;i < kArray.length;++i){
                    probability[i] = probability[i]/check;
                    probmap.put(kArray[i],probability[i]);
                }
            }
            for (int i = 0;i < kArray.length;++i){
                cdf += probability[i];
                map.put(kArray[i],cdf);
            }
        }

        private int generateUserDefineInt(){
//            Use cdf-Map for generation
            double randSeed = Math.random();
            int randNum = 0;
            Iterator<Map.Entry<Integer,Double>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer,Double> entry = iterator.next();
                randNum = entry.getKey();
                if(entry.getValue() > randSeed){
                    break;
                }
            }
            return randNum;
        }
    }

    private class Poisson {
        private int lambda;

        private void createPoissonMap(){
            double cdf = 0, factorial = 1, prob=0;

//            k=0 is a special case
            prob = Math.exp(-lambda);
            cdf += prob;
            probmap.put(0,prob);
            map.put(0, cdf);

            for (int k = 1; cdf < 1.0; k++) {
                factorial = 1;
                for (int i = 1; i <= k; i++){
                    factorial *= i;
                }
                prob = Math.pow(lambda,k) / factorial * Math.exp(-lambda) ;
//                System.out.println(Integer.toString(k)+','+Double.toString(prob));
                if (prob>0.0 && prob<1.0) {
                    cdf += prob;
                    probmap.put(k, prob);
                    map.put(k, cdf);
                }
//                System.out.println(k+"\t"+cdf);
                if ( cdf>0.9 && cdf<0.99 && !(prob>=0.0 && prob<1) ) System.exit(500);
                if ( cdf>0.99 && !(prob>=0.0 && prob<1)) break;
            }
        }

        private int generatePoisson() {
//            Junhao, based on Knuth poisson random
            final int STEP = 500;
            int k = 0; double L = lambda; double p = 1;
            do {
                k++;
                double u = Math.random();
                p *= u;
                while(p<1 && L>0) {
                    if (L > STEP){
                        p *= Math.exp(STEP);
                        L -= STEP;
                    } else {
                        p *= Math.exp(L);
                        L = 0;
                    }
                }
            } while(p>1);
//            System.out.println(k);
            return k-1;
        }
    }

    private class Zipf {
        private double s;
        private double N;

        private void createZipfMap(){
            double cdf = 0, hn = 0;
            for (int i = 1; i <= N; i++) {
                hn += 1.0/Math.pow(i,s);
            }

            for (int k = 1; k<=N; k++) {
                double prob = 1/(Math.pow(k,s)) / hn;
//                System.out.println(Integer.toString(k)+'\t'+Double.toString(prob));
                if (prob>0.0 && prob<1.0){
                    cdf += prob;
                    probmap.put(k,prob);
                    map.put(k, cdf);
                }
//                System.out.println(k+"\t"+cdf);
            }
        }

        private int generateZipf(){
//            Use Map for generation
            double randSeed = Math.random();
            int randNum = 0;
            Iterator<Map.Entry<Integer,Double>> iterator = map.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Integer,Double> entry = iterator.next();
                randNum = entry.getKey();
                if(entry.getValue() > randSeed){
                    break;
                }
            }
            return randNum;
        }
    }

    private class Gaussian {
        private double mu;
        private double sigma;
        synchronized private double generateGaussian() {
//            Use Box-Muller Algorithm
//            ATTENTION, in this method, the random-num will be generated in couple.
//            But we are supposed to handle them one by ony, so we should use synchronized.
            if (haveNextGaussian) {
                haveNextGaussian = false;
                return nextGaussian;
            }
            double u1, u2, z0, z1;
            u1 = Math.random() ;     // u1 in [0,1]
            u2 = Math.random() ;     // u2 in [0,1]
            z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0*3.14159265358979323846 * u2);
            z1 = Math.sqrt(-2.0 * Math.log(u1)) * Math.sin(2.0*3.14159265358979323846 * u2);
            double gs1 = z0 * sigma + mu;
            nextGaussian = z1 * sigma + mu;
            haveNextGaussian = true;
//            System.out.println(z0 * sigma + mu);
//            System.out.println(z1 * sigma + mu);
            return gs1;
        }
    }

    private class Exponential {
        private double lambda;
        private double generateExponential (){
//            Use Inverse function for generation
            double randSeed = Math.random();
            return  -Math.log(1.0-randSeed)/lambda;
        }
    }

    private class UniformDouble {
        //        ATTENTION, it's range area is [low,high)
        private double low, high;
        private double generateUniformDouble () {
            return low + (high - low) * Math.random();
        }
    }

    private class UniformInt {
        //        ATTENTION, it's range area is [low,high)
        private int low, high;
        private int generateUniformInt () {
            return (int) (low + (high - low) * Math.random());
        }

        private void createUniformIntMap(){
            double cdf = 0;
            double prob = 1.0/(high-low);
            for (int k = low; k < high; k++) {
                cdf += prob;
                probmap.put(k,prob);
                map.put(k, cdf);
//                System.out.println(k+"\t"+cdf);
            }
        }
    }

    private synchronized void deal(int randNum){
//        statistic the shown frequency of number x
        if (statisticInt.get(randNum) != null) {
            int a = statisticInt.get(randNum);
            statisticInt.replace(randNum, a+1);
        }else {
            statisticInt.put(randNum,1);
        }
//        statistic the total data amount
        count++;
//        output it into file when you think you've got enough data
        if (count == 5000) {
            try {
                System.out.println("Outputting data to D:/abc*.txt");
                FileWriter writer = new FileWriter("D:/zipf.txt");
                Iterator<Map.Entry<Integer,Integer>> iterator = statisticInt.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Integer,Integer> entry = iterator.next();
                    int k = entry.getKey();
                    int v = entry.getValue();
                    writer.write(Integer.toString(k)+"\t"+ Integer.toString(v)+'\n');
                }
                writer.close();
                System.out.println("Done");
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.exit(0);
        }
    }

    private synchronized void deal(double randNum){
//        Statistic the shown frequency of number x
//        Keep 2 digit to decimal places
        BigDecimal bd = new BigDecimal(randNum);
        randNum = bd.setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue();
        if (statisticDouble.get(randNum) != null) {
            statisticDouble.replace(randNum, statisticDouble.get(randNum)+1);
        }else {
            statisticDouble.put(randNum,1);
        }
//        statistic the total data amount
        ++count;
//        output it into file when you think you've got enough data
        if (count == 10000) {
            try {
                System.out.println("Outputting data to D:/abc*.txt");
                FileWriter writer = new FileWriter("D:/exponent.txt");
                Iterator<Map.Entry<Double,Integer>> iterator = statisticDouble.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Double,Integer> entry = iterator.next();
                    double k = entry.getKey();
                    int v = entry.getValue();
                    writer.write( Double.toString(k)+"\t"+Integer.toString(v)+'\n');
                }
                writer.close();
                System.out.println("Done");
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.exit(0);
        }
    }
}