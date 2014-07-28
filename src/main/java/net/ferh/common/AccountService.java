package net.ferh.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by ferh on 27.07.14.
 */
public interface AccountService extends Remote {
    /**
     * Retrieves current balance or zero if addAmount() method was not called before for specified id *
     *
     * @param id balance identifier
     */
    Long getAmount(Integer id) throws RemoteException;

    /**
     * Increases balance or set if addAmount() method was called first time *
     *
     * @param id    balance identifier
     * @param value positive or negative value, which must be added to current balance
     */
    void addAmount(Integer id, Long value) throws RemoteException;
}