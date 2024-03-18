package com.cioc.sync.repository;

import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import com.cioc.sync.entity.DaPrice;

@Repository
public interface DaPriceRepository extends MongoRepository<DaPrice, String> {
    @Query(value = "{'area' : ?0}", sort = "{_id : -1}")
    DaPrice findLatestDaPriceByArea(String area);
}
