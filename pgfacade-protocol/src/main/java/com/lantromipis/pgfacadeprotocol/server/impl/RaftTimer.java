package com.lantromipis.pgfacadeprotocol.server.impl;

import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Supplier;

public class RaftTimer {

    private final int initialDelay;
    private final int rate;
    private final Runnable actionOnTimeout;
    private final Supplier<Boolean> isRunningCheck;

    private final Timer timer;

    private boolean started = false;
    private boolean resetSignal = false;



    public RaftTimer(int initialDelay, int rate, Runnable actionOnTimeout, Supplier<Boolean> isRunningCheck) {
        this.initialDelay = initialDelay;
        this.rate = rate;
        this.actionOnTimeout = actionOnTimeout;
        this.isRunningCheck = isRunningCheck;
        timer = new Timer();
    }


    public void start() {
        if (started) {
            return;
        }

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (isRunningCheck.get()) {
                    if (!resetSignal) {
                        actionOnTimeout.run();
                    } else {
                        resetSignal = false;
                    }
                }
            }
        }, initialDelay, rate);
        started = true;
    }


    public void reset() {
        resetSignal = true;
    }


    public void stop() {
        timer.cancel();
        started = false;
    }
}
