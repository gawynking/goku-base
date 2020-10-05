package com.pgman.goku.springdatajpa.dao;

import com.pgman.goku.springdatajpa.domain.Role;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface RoleDao extends JpaRepository<Role,Long> ,JpaSpecificationExecutor<Role> {
}
