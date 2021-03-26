package com.pgman.goku.dp23.state.money;

public interface State {

    void checkEvent(Context context);

    void checkFailEvent(Context context);

    void makePriceEvent(Context context);

    void acceptOrderEvent(Context context);

    void notPeopleAcceptEvent(Context context);

    void payOrderEvent(Context context);

    void orderFailureEvent(Context context);

    void feedBackEvent(Context context);

    String getCurrentState();

}
