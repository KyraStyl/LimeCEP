package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class UsefulFunctions {
    public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.ITALY);

    public static Date castStrToDate(String datetocast){
        formatter.setTimeZone(TimeZone.getTimeZone("EET"));
        Date date = new Date();
        try{
            date = formatter.parse(datetocast);
        }catch (ParseException exception){
            System.out.println("Cannot parse "+datetocast+" because it is not a valid date! The date format is yyyy-MM-dd'T'HH:mm:ss.sss");
        }catch (NumberFormatException nexception){
            System.out.println("Cannot parse that "+datetocast);
        }
        return date;
    }

    public static final String capitalize(String str)
    {
        if (str == null || str.length() == 0) return str;
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static Long minutesToMillis(int minutes){
        return (long) minutes * 60 * 1000;
    }

    public static Long secondsToMillis(int seconds){
        return (long) seconds * 1000;
    }

}
