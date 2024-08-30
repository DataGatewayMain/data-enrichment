package app.vdb.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import app.vdb.entity.Vector19;

public interface Vector19Repository extends JpaRepository<Vector19, Long>, JpaSpecificationExecutor<Vector19> {
	   
	}
