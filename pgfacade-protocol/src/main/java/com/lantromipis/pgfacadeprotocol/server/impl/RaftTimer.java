package com.lantromipis.pgfacadeprotocol.server.impl;

import lombok.extern.slf4j.Slf4j;

import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Supplier;

@Slf4j
public class RaftTimer {

    private final int initialDelay;
    private final int rate;
    private final Runnable actionOnTimeout;
    private final Supplier<Boolean> isRunningCheck;

    private final Timer timer;

    private boolean started = false;
    private boolean resetSignal = false;
    private boolean paused = false;


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
                if (isRunningCheck.get() && !paused) {
                    if (!resetSignal) {
                        try {
                            actionOnTimeout.run();
                        } catch (Exception e) {
                            log.error("Exception in timer!", e);
                        }
                    } else {
                        resetSignal = false;
                    }
                }
            }
        }, initialDelay, rate);
        started = true;
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    public void reset() {
        resetSignal = true;
    }

    public void stop() {
        timer.cancel();
        started = false;
    }
}
