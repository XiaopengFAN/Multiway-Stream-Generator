import java.security.Key;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DSTuple {
    private DSModel[] models;

    DSTuple (DSModel[] models) {
        this.models = models;
    }


    public void produceTuple() {
        Map<String, String> tpmap = new HashMap<>();
        for (DSModel model : models) {
            if (model.getMd() == "Possion" ||
                    model.getMd() == "Zipf"||
                    model.getMd() == "UserDefine") {
                tpmap.put(model.getKey(), Integer.toString(model.startRamdomInt()));
            } else if (model.getMd() == "Gaussian"      ||
                    model.getMd() == "UniformDouble"    ||
                    model.getMd() == "Exponential") {
                tpmap.put(model.getKey(), Double.toString(model.startRamdomDouble()));
            }
        }

//        Output this tuple
        String text = "";
//        Check if we have the Key-Property that should be print out first.
        if (tpmap.containsKey("Key")){
//            text += ("Key" + "  " + tpmap.get("Key") +  "  " );
            text += (tpmap.get("Key") + ",");
            tpmap.remove("Key");
        }
//        Then we should print tuple's time stamp.
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        String time = df.format(new Date());
        String time = String.valueOf(new Date().getTime());
//        text += ("Time" + "  " + time +  "  " );
        text += (time + ",");

//        Finally the other properties
        for (Map.Entry<String, String> entry : tpmap.entrySet()) {
//            text += (entry.getKey() +  "  " + entry.getValue() + "  ");
            text += (entry.getValue() + ",");
        }
//        Here we should delete the last ','  (e.g. ,translate "1,2,3," to "1,2,3").
        text = text.substring(0,text.length()-1);

        System.out.println(text);
    }
}
