import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    private static boolean haveFound0 = false;
    private static boolean haveFound1 = false;
    private static boolean haveFound2 = false;

    public static void main(String args[]){
//        long period = Long.parseLong(args[0]);
//        int threadsAmout = Integer.parseInt(args[1]);
//        double selectivity = Double.parseDouble(args[2]);
//        int mode = Integer.parseInt(args[3]);       // 0 = Uniformal Distribution, 1 = Poisson, 2 = Zipf
//

        long period = 1;
        int threadsAmout = 10;
        double selectivity = 40.0;
        int mode = 3;

        double accuracy = 0.0;// the accuracy of selectivity
//        // if accuracy = 1, it means the final selectivity is in range of selectivity +- 1%
        DSModel[][] models = new DSModel[3][2];

//        Choose your model, and set parameters supposed
//        If you  want some user-defined model, then pass your value-set and probabilities-set.
//        If the total CDF is not 1, the algorithm will correct it automaticlly.
//        You can choose the origin model by using model.useXXXMODEL(ELEMENT_TYPE parameters).
        int[] kArray = {1,2,3};
        double[] probability = {1.0/3.0, 1.0/3.0, 1.0/3.0};
        models[0][0] = new DSModel();
        models[1][0] = new DSModel();
        models[2][0] = new DSModel();
//              *** ATTENTION, You need to change your model here.***
        models[0][0].useUserDefineInt("BusID",kArray, probability);
        models[1][0].useUserDefineInt("BusID",kArray, probability);
        models[2][0].useUserDefineInt("BusID",kArray, probability);

//        Calculate all parameters of models that match your selectivity.
        if (mode==0) {
            findParametersUniform(models,selectivity,accuracy);
        } else if (mode==1) {
            findParametersPoisson(models,selectivity,accuracy);
        } else if (mode==2) {
            accuracy = 1.0;
            findParametersZipf(models,selectivity,accuracy);
        } else if (mode==3) {
            accuracy = 1.0;
            findParametersUserDefined(models,selectivity,accuracy);
        }
//        System.exit(1);

//        Then send all your models into DSTuple, it will generate tuples for you.
//        You can change I/O format in it.
        DSTuple tuple1 = new DSTuple(models[0]);
        DSTuple tuple2 = new DSTuple(models[1]);
        DSTuple tuple3 = new DSTuple(models[2]);
        tuple1.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.
        tuple2.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.
        tuple3.setPrimaryKey("BusID");   // Set the primary key of tuple that will shown first.

//        Finally set threads' period and amount that you want.
//        Then pass your DSTuple and DSChoosemethod, start produce tuples！
        MultiwayStreamGenerator ds1 = new MultiwayStreamGenerator(period,threadsAmout, tuple1);
        ds1.start();
        MultiwayStreamGenerator ds2 = new MultiwayStreamGenerator(period,threadsAmout, tuple2);
        ds2.start();
        MultiwayStreamGenerator ds3 = new MultiwayStreamGenerator(period,threadsAmout, tuple3);
        ds3.start();
    }


    private static void findParametersUniform(DSModel[][] models, double selectivity,
                                              double accuracy){
        while(!haveFound0){
            if (accuracy > 5) System.exit(-233);
            for (int i = 0; i <= 10; i++) {
                for (int j = i+2; j <= 10; j++) {
                    for (int k = 0; k <= 10; k++) {
                        for (int l = k+2; l <= 10; l++) {
                            for (int m = 0; m <= 0; m++) {
                                for (int n = m+2; n <= 10; n++) {
                                    models[0][1] = new DSModel();
                                    models[1][1] = new DSModel();
                                    models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
                                    models[0][1].useUniformInt("AA",i,j);
                                    models[1][1].useUniformInt("BB",k,l);
                                    models[2][1].useUniformInt("CC",m,n);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
                                    DSChoosemethod[] chmethods = new DSChoosemethod[1];
                                    DSModel[] chKey0 = new DSModel[3];
                                    for (int s = 0; s < 3; s++) {
                                        chKey0[s] = models[s][1];
                                    }
                                    chmethods[0] = new DSChoosemethod(chKey0);
                                    double[] r = DSChoosemethod.calChooseRate(chmethods);
//                                    Check if these parameters match your selectivity
                                    if ( Math.abs(r[0]*100-selectivity)<=accuracy ){
                                        String text = "Format: Parameters of models, selectivity, amount of different elements of models \n";
                                        text += (Integer.toString(i)+",\t"+Integer.toString(j)+",\t"+
                                                Integer.toString(k)+",\t"+Integer.toString(l)+",\t"+
                                                Integer.toString(m)+",\t"+Integer.toString(n)+",\t"+
                                                Double.toString(r[0]*100.0)+'%'+
                                                ",\t"+models[0][1].getProbmap().size()+
                                                ",\t"+models[1][1].getProbmap().size()+
                                                ",\t"+models[2][1].getProbmap().size());

                                        // Output your log
                                        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                        String time = df.format(new Date());                  // Option 1
                                        String logFileName = ( "./" + time +  "," + "Uniform" + "," + Double.toString(r[0]*100.0)+'%'
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
                                        break;
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
            accuracy *= 2;
            if (accuracy == 0.0) accuracy += 0.1;
        }
    }

    private static void findParametersPoisson(DSModel[][] models, double selectivity, double accuracy){
        while(!haveFound1){
            if (accuracy > 5) System.exit(-233);
            for (int i = 1; i <=1000; i*=2) {
                for (int j = 1; j <=i; j*=2) {
                    for (int k = 1; k <= j; k*=2) {
                        models[0][1] = new DSModel();
                        models[1][1] = new DSModel();
                        models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
                        models[0][1].usePoisson("AA",i);
                        models[1][1].usePoisson("BB",j);
                        models[2][1].usePoisson("CC",k);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
                        DSChoosemethod[] chmethods = new DSChoosemethod[1];
                        DSModel[] chKey0 = new DSModel[3];
                        for (int s = 0; s < 3; s++) {
                            chKey0[s] = models[s][1];
                        }
                        chmethods[0] = new DSChoosemethod(chKey0);
                        double[] r = DSChoosemethod.calChooseRate(chmethods);

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
            accuracy *=2;
            if (accuracy == 0) accuracy += 0.1;
        }
    }

    private static void findParametersZipf(DSModel[][] models, double selectivity, double accuracy){
        while(!haveFound2){
            if (accuracy > 5) System.exit(-233);
            for (int i = 500; i <= 8000; i*=2) {
                for (double j = 1.4; j >= 0.2; j-=0.3) {
                    for (int k = 500; k <= i; k*=2) {
                        for (double l = 1.4; l >= 0.2; l-=0.3) {
                            for (int m = 10; m <= k; m*=2) {
                                for (double n = 1.4; n >= 0.2; n-=0.3) {
                                    models[0][1] = new DSModel();
                                    models[1][1] = new DSModel();
                                    models[2][1] = new DSModel();
//                                    *** ATTENTION, You need to change your model here.***
                                    models[0][1].useZipf("AA",i,j);
                                    models[1][1].useZipf("BB",k,l);
                                    models[2][1].useZipf("CC",m,n);

//        It's the choose method that you suppose, you can use it to optimize the choose-rate.
                                    DSChoosemethod[] chmethods = new DSChoosemethod[1];
                                    DSModel[] chKey0 = new DSModel[3];
                                    for (int s = 0; s < 3; s++) {
                                        chKey0[s] = models[s][1];
                                    }
                                    chmethods[0] = new DSChoosemethod(chKey0);
                                    double[] r = DSChoosemethod.calChooseRate(chmethods);
//                                    Check if these parameters match your selectivity
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
//            System.out.println(accuracy);
            accuracy *=2;
            if (accuracy == 0) accuracy += 0.1;
        }
    }

    private static void findParametersUserDefined(DSModel[][] models, double selectivity, double accuracy){
        while(!haveFound2){
            if (accuracy > 5) System.exit(-233);
            for (int i = 0; i <= 10; i++) {
                for (int j = i+2; j <= 10; j++) {
                    for (int k = 8; k <= 15; k++) {
                        for (double l = 0.5; l <= 1.5; l+=0.3) {
                            for (int m = 0; m <= 10; m++) {
                                for (double n = 0; n <= 0; n++) {
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
                                    DSChoosemethod[] chmethods = new DSChoosemethod[1];
                                    DSModel[] chKey0 = new DSModel[3];
                                    for (int s = 0; s < 3; s++) {
                                        chKey0[s] = models[s][1];
                                    }
                                    chmethods[0] = new DSChoosemethod(chKey0);
                                    double[] r = DSChoosemethod.calChooseRate(chmethods);
//                                    Check if these parameters match your selectivity
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
                                        String logFileName = ( "./" + time +  "," + "Zipf" + "," + Double.toString(r[0]*100.0)+'%'
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
//            System.out.println(accuracy);
            accuracy *=2;
            if (accuracy == 0) accuracy += 0.1;
        }
    }

}


