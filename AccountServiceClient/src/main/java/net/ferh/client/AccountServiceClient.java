package net.ferh.client;

import net.ferh.common.AccountService;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ferh on 27.07.14.
 */
public class AccountServiceClient {
    private static AccountService accountService;
    private static int maxId;
    private static long maxBalance = 500;
    private static volatile boolean running = true;

    public static void main(String[] args) {
        try {
            maxId = Integer.parseInt(args[2]);
            String name = "AccountService";
            Registry registry = LocateRegistry.getRegistry("localhost", 8080);
            accountService = (AccountService) registry.lookup(name);

            AccountServiceClient accountServiceClient = new AccountServiceClient();
            accountServiceClient.runReaders(Integer.parseInt(args[0]));
            accountServiceClient.runWriters(Integer.parseInt(args[1]));
        } catch (Exception e) {
            System.err.println("AccountService exception");
            e.printStackTrace();
        }
    }

    public void runWriters(int wCount) {
        ExecutorService executorService = Executors.newFixedThreadPool(wCount);
        for (int i = 0; i < wCount; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (running) {
                            accountService.addAmount(getRandInt(), getRandLong());
                        }
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    public void runReaders(int rCount) {
        ExecutorService executorService = Executors.newFixedThreadPool(rCount);
        for (int i = 0; i < rCount; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (running) {
                            accountService.getAmount(getRandInt());
                        }
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    public Integer getRandInt() {
        Random rnd = new Random();
        return Math.abs((rnd.nextInt() % maxId)) + 1;
    }

    public Long getRandLong() {
        Random rnd = new Random();
        return rnd.nextLong() % maxBalance;
    }
}
