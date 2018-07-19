import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DSMain {
    private static boolean haveFound0 = false;
    private static boolean haveFound1 = false;
    private static boolean haveFound2 = false;
    static String[] topics = {"streamA","streamB","streamC","streamD","streamE"};
    static String servers = "master:9092,slave01:9092,slave02:9092,slave03:9092,slave04:9092,slave06:9092,slave07:9092,slave08:9092,slave09:9092,slave10:9092,slave11:9092,slave12:9092";

    public static void main(String args[]){
        /* Code For jar */
        long period = Long.parseLong(args[0]);
        int threadsAmout = Integer.parseInt(args[1]);
        double selectivity = Double.parseDouble(args[2]);
        // 0 = Uniformal, 1 = Poisson, 2 = Zipf, 3 = Mixed, 999 = SelfDefUniform
        int joinMode = Integer.parseInt(args[3]);
        /* Code For jar Over */

        /* Code For Test */
//        long period = 10;
//        int threadsAmout = 1;
//        double selectivity = 10;
//        int joinMode = 0;
        /* Code For Test Over */

        double accuracy = 0.0;// the accuracy of selectivity
//        // if accuracy = 1, it means the final selectivity is in range of selectivity +- 1%
        DSModel[][] models = new DSModel[5][2];

//        Choose your model, and set parameters supposed
//        If you  want some user-defined model, then pass your value-set and probabilities-set.
//        If the total CDF is not 1, the algorithm will correct it automaticlly.
//        You can choose the origin model by using model.useXXXMODEL(ELEMENT_TYPE parameters).
        int[] kArray = {1,2,3,4,5};
        double[] probability = {1.0/5.0, 1.0/5.0, 1.0/5.0, 1.0/5.0, 1.0/5.0};
        models[0][0] = new DSModel();
        models[1][0] = new DSModel();
        models[2][0] = new DSModel();
        models[3][0] = new DSModel();
        models[4][0] = new DSModel();
//              *** ATTENTION, You need to change your model here.***
        models[0][0].useUserDefineInt("BusID",kArray, probability);
        models[1][0].useUserDefineInt("BusID",kArray, probability);
        models[2][0].useUserDefineInt("BusID",kArray, probability);
        models[3][0].useUserDefineInt("BusID",kArray, probability);
        models[4][0].useUserDefineInt("BusID",kArray, probability);

//        Calculate all parameters of models that match your selectivity.
            if (joinMode==0) {
                accuracy = 1.0;
                findParametersUniform(models,selectivity,accuracy);
            } else if (joinMode==1) {
                findParametersPoisson(models,selectivity,accuracy);
            } else if (joinMode==2) {
                accuracy = 1.0;
                findParametersZipf(models,selectivity,accuracy);
            } else if (joinMode==3) {
                accuracy = 1.0;
                findParametersUserDefined(models,selectivity,accuracy);
            } else if (joinMode == 999) {
                accuracy = 5.0;
                selfDefParamUniform(models,selectivity,accuracy,
                            Integer.parseInt(args[4]),Integer.parseInt(args[5])
                            ,Integer.parseInt(args[6]),Integer.parseInt(args[7])
                            ,Integer.parseInt(args[8]),Integer.parseInt(args[9])
                            ,Integer.parseInt(args[10]),Integer.parseInt(args[11])
                            ,Integer.parseInt(args[12]),Integer.parseInt(args[13]));
            }
//        System.exit(1);

//        Then send all your models into DSTuple, it will generate tuples for you.
//        You can change I/O format in it.
        DSTuple tuple1 = new DSTuple(models[0]);
        DSTuple tuple2 = new DSTuple(models[1]);
        DSTuple tuple3 = new DSTuple(models[2]);
        DSTuple tuple4 = new DSTuple(models[3]);
        DSTuple tuple5 = new DSTuple(models[4]);


        tuple1.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.
        tuple2.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.
        tuple3.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.
        tuple4.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.
        tuple5.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.

//        Finally set threads' period and amount that you want.
//        Then pass your DSTuple and DSJoin, start produce tuples！

        MultiwayStreamGenerator ds1 = new MultiwayStreamGenerator(period,threadsAmout, tuple1,topics[0],servers);
        ds1.start();
        MultiwayStreamGenerator ds2 = new MultiwayStreamGenerator(period,threadsAmout, tuple2,topics[1],servers);
        ds2.start();
        MultiwayStreamGenerator ds3 = new MultiwayStreamGenerator(period,threadsAmout, tuple3,topics[2],servers);
        ds3.start();
        MultiwayStreamGenerator ds4 = new MultiwayStreamGenerator(period, threadsAmout, tuple4,topics[3],servers);
        ds4.start();
        MultiwayStreamGenerator ds5 = new MultiwayStreamGenerator(period, threadsAmout, tuple5,topics[4],servers);
        ds5.start();
    }

    private static void selfDefParamUniform(DSModel[][] models, double selectivity, double accuracy,
                                            int i, int j, int k, int l, int m, int n, int o, int p, int q, int r) {
        models[0][1] = new DSModel();
        models[1][1] = new DSModel();
        models[2][1] = new DSModel();
        models[3][1] = new DSModel();
        models[4][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
        models[0][1].useUniformInt("AA", i, j);
        models[1][1].useUniformInt("BB", k, l);
        models[2][1].useUniformInt("CC", m, n);
        models[3][1].useUniformInt("DD", o, p);
        models[4][1].useUniformInt("EE", q, r);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
        DSJoin[] joins = new DSJoin[1];
        DSModel[] chKey0 = new DSModel[models.length];
        for (int s = 0; s < models.length; s++) {
            chKey0[s] = models[s][1];
        }
        joins[0] = new DSJoin(chKey0);
        double[] rate = DSJoin.calChooseRate(joins);
//                                    Check if these parameters match your selectivity
        if (Math.abs(rate[0] * 100 - selectivity) <= accuracy) {
            String text = "Format: Parameters of models, selectivity, amount of different elements of models \n";
            text += (Integer.toString(i) + ",\t" + Integer.toString(j) + ",\t" +
                    Integer.toString(k) + ",\t" + Integer.toString(l) + ",\t" +
                    Integer.toString(m) + ",\t" + Integer.toString(n) + ",\t" +
                    Integer.toString(o) + ",\t" + Integer.toString(p) + ",\t" +
                    Integer.toString(q) + ",\t" + Integer.toString(r) + ",\t" +
                    Double.toString(rate[0] * 100.0) + '%' +
                    ",\t" + models[0][1].getProbmap().size() +
                    ",\t" + models[1][1].getProbmap().size() +
                    ",\t" + models[2][1].getProbmap().size() +
                    ",\t" + models[3][1].getProbmap().size() +
                    ",\t" + models[4][1].getProbmap().size());

            // Output your log
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String time = df.format(new Date());                  // Option 1
            String logFileName = ("./" + time + "," + "Uniform" + "," + Double.toString(selectivity) + '%'
                    + ".log");
            try {
                FileWriter writer = new FileWriter(logFileName);
                writer.write(text);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(text);
        } else {
            System.out.println("The Params you set is "+rate[0]*100+ "% ,which is not satisfied to your selectivity");
            System.exit(-233);
        }
    }


    private static void findParametersUniform(DSModel[][] models, double selectivity,
                                              double accuracy){
        int[] params = new int[10];
        double min = 999;
        final int upper = 10;
        for (int i = 0; i <= upper; i++) {
            for (int j = i+2; j <= upper; j++) {
                for (int k = 0; k <= upper; k++) {
                    for (int l = k+2; l <= upper; l++) {
                        for (int m = 0; m <= upper; m++) {
                            for (int n = m+2; n <= upper; n++) {
                                for (int o = 0; o <=upper; ++o) {
                                    for (int p = o+2; p <= upper; ++p) {
                                        for (int q = 0; q <= upper; ++q) {
                                            for (int r = q+2; r <= upper; ++r){
                                                models[0][1] = new DSModel();
                                                models[1][1] = new DSModel();
                                                models[2][1] = new DSModel();
                                                models[3][1] = new DSModel();
                                                models[4][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
                                                models[0][1].useUniformInt("AA",i,j);
                                                models[1][1].useUniformInt("BB",k,l);
                                                models[2][1].useUniformInt("CC",m,n);
                                                models[3][1].useUniformInt("DD",o,p);
                                                models[4][1].useUniformInt("EE",q,r);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
                                                DSJoin[] joins = new DSJoin[1];
                                                DSModel[] chKey0 = new DSModel[models.length];
                                                for (int s = 0; s < models.length; s++) {
                                                    chKey0[s] = models[s][1];
                                                }
                                                joins[0] = new DSJoin(chKey0);
                                                double[] rate = DSJoin.calChooseRate(joins);
                                                if (min > Math.abs(rate[0]*100-selectivity)){
                                                    min = Math.abs(rate[0]*100-selectivity);
                                                    params[0]=i;
                                                    params[1]=j;
                                                    params[2]=k;
                                                    params[3]=l;
                                                    params[4]=m;
                                                    params[5]=n;
                                                    params[6]=o;
                                                    params[7]=p;
                                                    params[8]=q;
                                                    params[9]=r;
                                                }
//                                    Check if these parameters match your selectivity
                                                if ( Math.abs(rate[0]*100-selectivity)<=accuracy ){
                                                    String text = "Format: Parameters of models, selectivity, amount of different elements of models \n";
                                                    text += (Integer.toString(i)+",\t"+Integer.toString(j)+",\t"+
                                                            Integer.toString(k)+",\t"+Integer.toString(l)+",\t"+
                                                            Integer.toString(m)+",\t"+Integer.toString(n)+",\t"+
                                                            Integer.toString(o)+",\t"+Integer.toString(p)+",\t"+
                                                            Integer.toString(q)+",\t"+Integer.toString(r)+",\t"+
                                                            Double.toString(rate[0]*100.0)+'%'+
                                                            ",\t"+models[0][1].getProbmap().size()+
                                                            ",\t"+models[1][1].getProbmap().size()+
                                                            ",\t"+models[2][1].getProbmap().size()+
                                                            ",\t"+models[3][1].getProbmap().size()+
                                                            ",\t"+models[4][1].getProbmap().size());

                                                    // Output your log
                                                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                                    String time = df.format(new Date());                  // Option 1
                                                    String logFileName = ( "./" + time +  "," + "Uniform" + "," + Double.toString(selectivity)+'%'
                                                            + ".log");
                                                    try {
                                                        FileWriter writer = new FileWriter(logFileName);
                                                        writer.write(text);
                                                        writer.close();
                                                    } catch (IOException e) {
                                                        e.printStackTrace();
                                                    }

                                                    System.out.println(text);
//                                        System.exit(666);
                                                    haveFound0 = true;
                                                    break;}
                                            }
                                            if (haveFound0) break;
                                        }
                                        if (haveFound0) break;
                                    }
                                    if (haveFound0) break;
                                }
                                if (haveFound0) break;
                            }
                            if (haveFound0) break;
                        }
                        if (haveFound0) break;
                    }
                    if (haveFound0) break;
                }
                if (haveFound0) break;
            }
            if (haveFound0) break;
        }
            // If we can not find parameters that match your accuracy. We'll try a bigger accuracy.
        if (min<=accuracy){
            return;
        } else if (min <5){
            models[0][1] = new DSModel();
            models[1][1] = new DSModel();
            models[2][1] = new DSModel();
            models[3][1] = new DSModel();
            models[4][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
            models[0][1].useUniformInt("AA",params[0],params[1]);
            models[1][1].useUniformInt("BB",params[2],params[3]);
            models[2][1].useUniformInt("CC",params[4],params[5]);
            models[3][1].useUniformInt("DD",params[6],params[7]);
            models[4][1].useUniformInt("EE",params[8],params[9]);
        } else {
            System.out.println("Can Not Find Matched Params.");
            System.exit(-233);
        }
    }

    private static void findParametersPoisson(DSModel[][] models, double selectivity, double accuracy){
        int[] params = new int[3];
        double min = 999;
        for (int i = 1; i <=15; i++) {
            for (int j = 1; j <=i; j++) {
                for (int k = 1; k <= j; k++) {
                    models[0][1] = new DSModel();
                    models[1][1] = new DSModel();
                    models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
                    models[0][1].usePoisson("AA",i);
                    models[1][1].usePoisson("BB",j);
                    models[2][1].usePoisson("CC",k);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
                    DSJoin[] joins = new DSJoin[1];
                    DSModel[] chKey0 = new DSModel[3];
                    for (int s = 0; s < 3; s++) {
                        chKey0[s] = models[s][1];
                    }
                    joins[0] = new DSJoin(chKey0);
                    double[] r = DSJoin.calChooseRate(joins);
                    if (min > Math.abs(r[0]*100-selectivity)){
                        min = Math.abs(r[0]*100-selectivity);
                        params[0]=i;
                        params[1]=j;
                        params[2]=k;
                    }
//                                    Check if these parameters match your selectivity
                    if ( Math.abs(r[0]*100-selectivity)<=accuracy ){
                        String text = "Format: Parameters of models, selectivity, amount of different elements of models \n";
                        text += (Integer.toString(i)+",\t"+Double.toString(j)+",\t"+
                                Integer.toString(k)+",\t"+
                                Double.toString(r[0]*100.0)+'%'+
                                ",\t"+models[0][1].getProbmap().size()+
                                ",\t"+models[1][1].getProbmap().size()+
                                ",\t"+models[2][1].getProbmap().size());

                        // Output your log
                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String time = df.format(new Date());                  // Option 1
                        String logFileName = ( "./" + time +  "," + "Poisson" + "," + Double.toString(r[0]*100.0)+'%'
                                + ".log");
                        try {
                            FileWriter writer = new FileWriter(logFileName);
                            writer.write(text);
                            writer.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
//                            System.out.println(text);
                        haveFound1 = true;
                        break;
                    }
                }
                if (haveFound1) break;
            }
            if (haveFound1) break;
        }
            // If we can not find parameters that match your accuracy. We'll try a bigger accuracy.
        if (min<=accuracy){
            return;
        } else if (min <5){
            models[0][1] = new DSModel();
            models[1][1] = new DSModel();
            models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
            models[0][1].usePoisson("AA",params[0]);
            models[1][1].usePoisson("BB",params[1]);
            models[2][1].usePoisson("CC",params[2]);
        } else {
            System.out.println("Can Not Find Matched Params.");
            System.exit(-233);
        }
    }

    private static void findParametersZipf(DSModel[][] models, double selectivity, double accuracy){
        int[] paramsInt = new int[3];
        double[] paramsDouble = new double[3];
        double min = 999;
        for (int i = 500; i <= 8000; i*=2) {
            for (double j = 0.3; j <1.5; j+=0.3) {
                for (int k = 500; k <= i; k*=2) {
                    for (double l = 0.3; l < 1.5; l+=0.3) {
                        for (int m = 10; m <= k; m*=2) {
                            for (double n = 0.3; n < 1.5; n+=0.3) {
                                models[0][1] = new DSModel();
                                models[1][1] = new DSModel();
                                models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
                                models[0][1].useZipf("AA",i,j);
                                models[1][1].useZipf("BB",k,l);
                                models[2][1].useZipf("CC",m,n);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
                                DSJoin[] joins = new DSJoin[1];
                                DSModel[] chKey0 = new DSModel[3];
                                for (int s = 0; s < 3; s++) {
                                    chKey0[s] = models[s][1];
                                }
                                joins[0] = new DSJoin(chKey0);
                                double[] r = DSJoin.calChooseRate(joins);
//                                    Check if these parameters match your selectivity
                                if (min > Math.abs(r[0]*100-selectivity)){
                                    min = Math.abs(r[0]*100-selectivity);
                                    paramsInt[0]=i;
                                    paramsDouble[0]=j;
                                    paramsInt[1]=k;
                                    paramsDouble[1]=l;
                                    paramsInt[2]=m;
                                    paramsDouble[2]=n;
                                }
                                if ( Math.abs(r[0]*100-selectivity)<=accuracy ){
                                    String text = "Format: Parameters of models, selectivity, amount of different elements of models \n";
                                    text += (Integer.toString(i)+",\t"+Double.toString(j)+",\t"+
                                            Integer.toString(k)+",\t"+Double.toString(l)+",\t"+
                                            Integer.toString(m)+",\t"+Double.toString(n)+",\t"+
                                            Double.toString(r[0]*100.0)+'%'+
                                            ",\t"+models[0][1].getProbmap().size()+
                                            ",\t"+models[1][1].getProbmap().size()+
                                            ",\t"+models[2][1].getProbmap().size());

                                    // Output your log
                                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                    String time = df.format(new Date());                  // Option 1
                                    String logFileName = ( "./" + time +  "," + "Zipf" + "," + Double.toString(r[0]*100.0)+'%'
                                            + ".log");
                                    try {
                                        FileWriter writer = new FileWriter(logFileName);
                                        writer.write(text);
                                        writer.close();
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
//                                        System.out.println(text);
                                    haveFound2 = true;
                                    break;
                                }
                            }
                            if (haveFound2) break;
                        }
                        if (haveFound2) break;
                    }
                    if (haveFound2) break;
                }
                if (haveFound2) break;
            }
            if (haveFound2) break;
        }
            // If we can not find parameters that match your accuracy. We'll try a bigger accuracy.
        if (min<=accuracy){
            return;
        } else if (min <5){
            models[0][1] = new DSModel();
            models[1][1] = new DSModel();
            models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
            models[0][1].useZipf("AA",paramsInt[0],paramsDouble[0]);
            models[1][1].useZipf("BB",paramsInt[1],paramsDouble[1]);
            models[2][1].useZipf("CC",paramsInt[2],paramsDouble[2]);
        } else {
            System.out.println("Can Not Find Matched Params.");
            System.exit(-233);
        }
    }

    private static void findParametersUserDefined(DSModel[][] models, double selectivity, double accuracy){
        int[] paramsInt = new int[5];
        double[] paramsDouble = new double[1];
        double min = 999;
        for (int i = 0; i <= 10; i++) {
            for (int j = i+2; j <= 10; j++) {
                for (int k = 8; k <= 15; k++) {
                    for (double l = 0.5; l <= 1.5; l+=0.3) {
                        for (int m = 0; m <= 10; m++) {
                            models[0][1] = new DSModel();
                            models[1][1] = new DSModel();
                            models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
                            models[0][1].useUniformInt("AA",i,j);
                            models[1][1].useZipf("BB",k,l);
                            models[2][1].usePoisson("CC",m);

                            if (Math.abs(models[0][1].getProbmap().size()*
                                    models[1][1].getProbmap().size()*
                                    models[2][1].getProbmap().size()-1000)>50) {
                                continue;
                            }

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
                            DSJoin[] joins = new DSJoin[1];
                            DSModel[] chKey0 = new DSModel[3];
                            for (int s = 0; s < 3; s++) {
                                chKey0[s] = models[s][1];
                            }
                            joins[0] = new DSJoin(chKey0);
                            double[] r = DSJoin.calChooseRate(joins);
//                                    Check if these parameters match your selectivity
                            if (min > r[0]*100-selectivity){
                                min = r[0]*100-selectivity;
                                paramsInt[0]=i;
                                paramsInt[0]=j;
                                paramsInt[1]=k;
                                paramsDouble[0]=l;
                                paramsInt[2]=m;
                            }
                            if ( Math.abs(r[0]*100-selectivity)<=accuracy ){
                                String text = "Format: Parameters of models, selectivity, amount of different elements of models \n";
                                text += (Integer.toString(i)+",\t"+Double.toString(j)+",\t"+
                                        Integer.toString(k)+",\t"+Double.toString(l)+",\t"+
                                        Integer.toString(m)+",\t"+
                                        Double.toString(r[0]*100.0)+'%'+
                                        ",\t"+models[0][1].getProbmap().size()+
                                        ",\t"+models[1][1].getProbmap().size()+
                                        ",\t"+models[2][1].getProbmap().size());

                                // Output your log
                                SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                String time = df.format(new Date());                  // Option 1
                                String logFileName = ( "./" + time +  "," + "Mixed" + "," + Double.toString(r[0]*100.0)+'%'
                                        + ".log");
                                try {
                                    FileWriter writer = new FileWriter(logFileName);
                                    writer.write(text);
                                    writer.close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                                System.out.println(text);
//                                        System.exit(-1);
                                haveFound2 = true;
                                break;
                            }
                            if (haveFound2) break;
                        }
                        if (haveFound2) break;
                    }
                    if (haveFound2) break;
                }
                if (haveFound2) break;
            }
            if (haveFound2) break;
        }
            // If we can not find parameters that match your accuracy. We'll try a bigger accuracy.
        if (min<=accuracy){
            return;
        } else if (min <5){
            models[0][1] = new DSModel();
            models[1][1] = new DSModel();
            models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
            models[0][1].useUniformInt("AA",paramsInt[0],paramsInt[1]);
            models[1][1].useZipf("BB",paramsInt[2],paramsDouble[0]);
            models[2][1].usePoisson("CC",paramsInt[3]);
        } else {
            System.out.println("Can Not Find Matched Params.");
            System.exit(-233);
        }
    }

}


