package com.lucho.spark;

import com.lucho.domain.Log;
import com.lucho.utils.Utils;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.List;

public class LogsFunction implements VoidFunction<Log>, Serializable {
    Broadcast<List<String>> broadcastvar;
    public LogsFunction(Broadcast<List<String>> broadcastvar) {
        this.broadcastvar = broadcastvar;
    }

    @Override
    public void call(Log log) throws Exception {

        String jsonLog = Utils.jsonLog(log);
        broadcastvar.value().add(jsonLog);

    }
}
