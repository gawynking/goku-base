package com.pgman.goku.dsaa3e;

public interface HashFamily<AnyType>
{
    int hash(AnyType x, int which);
    int getNumberOfFunctions();
    void generateNewFunctions();
}
