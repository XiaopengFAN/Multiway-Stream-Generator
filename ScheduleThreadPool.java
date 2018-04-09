package Islotus;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import java.util.Random;
import java.text.SimpleDateFormat;

//MyThreadPool为线程池管理类
//MyThread为实际需要运行的线程类

class ScheduleThreadPool {
    private String hostGps;
    private int portGps;
    private Socket clientGps;
    private OutputStream outGps;

    private String hostMeter;
    private int portMeter;
    private Socket clientMeter;
    private OutputStream outMeter;

    private ConfClass conf;
    private Random randFloat;
    private SimpleDateFormat sdf;

    private ScheduledExecutorService scheduledTPExeService;

    ScheduleThreadPool(ConfClass conf) {
        try {
            this.hostGps = "10.42.43.10";
            this.portGps = 8800;
            this.clientGps = new Socket(hostGps, portGps);
            this.outGps = clientGps.getOutputStream();

            this.hostMeter = "10.42.43.10";
            this.portMeter = 8899;
            this.clientMeter = new Socket(hostMeter, portMeter);
            this.outMeter = clientMeter.getOutputStream();

            this.conf = conf;
            this.randFloat = new java.util.Random();
            this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");

            this.scheduledTPExeService = Executors.newScheduledThreadPool(conf.getPoolSize());

        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
//        catch(Exception e) {
//            e.printStackTrace();
//        }
    }

    void doTask() {
        try {
            for (int i = 0; i < conf.getPoolSize(); ++i) {
                String plateId = conf.getPlateId(5);
                CommandGps commandGps = new CommandGps(i, plateId);
                CommandMeter commandMeter = new CommandMeter(plateId);
                scheduledTPExeService.scheduleAtFixedRate(commandGps, 0, conf.getThreadConf()[i][0], TimeUnit.MILLISECONDS);
                scheduledTPExeService.scheduleAtFixedRate(commandMeter, 0, conf.getThreadConf()[i][1], TimeUnit.MILLISECONDS);
            }
            try {
                // waits for termination for 30 seconds only
                scheduledTPExeService.awaitTermination(conf.getAliveTime(), TimeUnit.SECONDS);
                scheduledTPExeService.shutdown();
                System.out.println("Shutdown Complete");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } finally {
            //System.out.println("get into finally");
            try {
                outGps.close();
                clientGps.close();

                outMeter.close();
                clientMeter.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            //System.out.println("finish!");
        }

        // shutdown now.
        //scheduledTPExeService.shutdownNow();
        //scheduledTPExeService.shutdown();
        //System.out.println("Shutdown Complete");
    }

    class CommandGps implements Runnable
    {
        private int id;
        private String plateId;

       CommandGps(int id, String plateId) {
            try {
                this.id = id;
                this.plateId = plateId;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        public void run() {
            try {
                int groupId = conf.getThreadConf()[id][2];
                String info = plateId + "," + sdf.format(System.currentTimeMillis()) + "," + Integer.toString(groupId) + "\n";
                // there must be "\n" at the end of string
                outGps.write(info.getBytes());
                outGps.flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    class CommandMeter implements Runnable
    {
        private String plateId;

        CommandMeter(String plateId) {
            try {
                this.plateId = plateId;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        public void run() {
            try {
                String joinString = String.valueOf((int) (randFloat.nextFloat() * 100));
                String info = plateId + "," + sdf.format(System.currentTimeMillis()) + "," + joinString + "\n";
                // there must be "\n" at the end of string
                outMeter.write(info.getBytes());
                outMeter.flush();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
