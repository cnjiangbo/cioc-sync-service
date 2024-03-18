package com.cioc.sync.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cioc.sync.entity.DaPrice;
import com.cioc.sync.repository.DaPriceRepository;
import com.cioc.sync.service.DaPriceService;

@Service
public class DaPriceServiceImpl implements DaPriceService {

    @Autowired
    DaPriceRepository daPriceRepository;

    @SuppressWarnings("null")
    @Override
    public DaPrice createOrUpdateTask(DaPrice daPrice) {
        return daPriceRepository.save(daPrice);
    }

}
