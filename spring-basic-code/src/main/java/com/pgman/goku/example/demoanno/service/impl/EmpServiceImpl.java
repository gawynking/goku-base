package com.pgman.goku.example.demoanno.service.impl;

import com.pgman.goku.example.demoanno.dao.IEmpDao;
import com.pgman.goku.example.demoanno.domain.Emp;
import com.pgman.goku.example.demoanno.service.IEmpService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;

@Service("empService")
public class EmpServiceImpl implements IEmpService {

    @Autowired
    @Qualifier("empDao")
    private IEmpDao empDao;

    public List<Emp> findAllEmp() {
        return empDao.findAllEmp();
    }

    public Emp findById(Integer empno) {
        return empDao.findById(empno);
    }

    public void saveEmp(Emp emp) {
        empDao.saveEmp(emp);
    }

    public void updateEmp(Emp emp) {
        empDao.updateEmp(emp);
    }

    public void deleteEmp(Integer empno) {
        empDao.deleteEmp(empno);
    }

}
