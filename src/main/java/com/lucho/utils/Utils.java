package com.lucho.utils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucho.domain.Log;

import java.io.Serializable;


public class Utils implements Serializable {



    public static String jsonLog(Log log) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        String json = "";
        try {
            json = mapper.writeValueAsString(log);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return json;
    }

}