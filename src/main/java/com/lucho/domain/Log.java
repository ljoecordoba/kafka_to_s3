package com.lucho.domain;


import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;
public class Log implements Serializable
{
    //    Logger logger = LoggerFactory.getLogger(Log.class.getName());
    String logchannel                 = ""  ;
    String formatversion              = ""  ;
    String eventid                    = ""  ;
    String timestamp                  = ""  ;
    String companyid                  = ""  ;
    String applianceid                = ""  ;
    String appliancemachinename       = ""  ;
    String realm                      = ""  ;
    String userid                     = ""  ;
    String hasheduserid               = ""  ;
    String useragent                  = ""  ;
    String userhostaddress            = ""  ;
    String producttype                = ""  ;
    String receivetoken               = ""  ;
    String usejava                    = ""  ;
    String allowedtoken               = ""  ;
    String authguimode                = ""  ;
    String authregmethod              = ""  ;
    String authregmethodinfo          = ""  ;
    String ispreauth                  = ""  ;
    String preauthpage                = ""  ;
    String destinationsiteurl         = ""  ;
    String returnurl                  = ""  ;
    String targeturl                  = ""  ;
    String samlconsumersiteurl        = ""  ;
    String samlrelaystate             = ""  ;
    String samltargeturl              = ""  ;
    String succeed                    = ""  ;
    String comment                    = ""  ;
    String analyzeengineresult        = ""  ;
    String browsersession             = ""  ;
    String statemachineid             = ""  ;
    String requestid                  = ""  ;
    String message                    = ""  ;

    public Log(String rawMessage){
        List<String>  matchList = Arrays.asList(rawMessage.split("\""));
        System.out.println("El tamaño de la lista es: " + matchList.size());
        try{
            logchannel            = matchList.get(1).replace("\\","");
            formatversion         = matchList.get(3).replace("\\","");
            eventid               = matchList.get(5).replace("\\","");
            timestamp             = matchList.get(7).replace("\\","");
            companyid             = matchList.get(9).replace("\\","");
            applianceid           = matchList.get(11).replace("\\","");
            appliancemachinename  = matchList.get(13).replace("\\","");
            realm                 = matchList.get(15).replace("\\","");
            userid                = matchList.get(17).replace("\\","");
            hasheduserid          = matchList.get(19).replace("\\","");
            useragent             = matchList.get(21).replace("\\","");
            userhostaddress       = matchList.get(23).replace("\\","");
            producttype           = matchList.get(25).replace("\\","");
            receivetoken          = matchList.get(27).replace("\\","");
            usejava               = matchList.get(29).replace("\\","");
            allowedtoken          = matchList.get(31).replace("\\","");
            authguimode           = matchList.get(33).replace("\\","");
            authregmethod         = matchList.get(35).replace("\\","");
            authregmethodinfo     = matchList.get(37).replace("\\","");
            ispreauth             = matchList.get(39).replace("\\","");
            preauthpage           = matchList.get(41).replace("\\","");
            destinationsiteurl    = matchList.get(43).replace("\\","");
            returnurl             = matchList.get(45).replace("\\","");
            targeturl             = matchList.get(47).replace("\\","");
            samlconsumersiteurl   = matchList.get(49).replace("\\","");
            samlrelaystate        = matchList.get(51).replace("\\","");
            samltargeturl         = matchList.get(53).replace("\\","");
            succeed               = matchList.get(55).replace("\\","");
            comment               = matchList.get(57).replace("\\","");
            analyzeengineresult   = matchList.get(59).replace("\\","");
            browsersession        = matchList.get(61).replace("\\","");
            statemachineid        = matchList.get(63).replace("\\","");
            requestid             = matchList.get(65).replace("\\","");
            message               = matchList.get(67).replace("\\","");
        }
        catch (IndexOutOfBoundsException e){
            System.out.println("An error has occurred at filling the fields of the log");
        }

    }

}
