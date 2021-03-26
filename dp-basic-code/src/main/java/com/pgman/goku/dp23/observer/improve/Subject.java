package com.pgman.goku.dp23.observer.improve;

public interface Subject {
	
	public void registerObserver(Observer o);
	public void removeObserver(Observer o);
	public void notifyObservers();

}
