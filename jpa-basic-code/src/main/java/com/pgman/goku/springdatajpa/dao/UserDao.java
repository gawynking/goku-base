package com.pgman.goku.springdatajpa.dao;

import com.pgman.goku.springdatajpa.domain.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface UserDao extends JpaRepository<User,Long> ,JpaSpecificationExecutor<User> {
}
