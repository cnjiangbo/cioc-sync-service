package com.cioc.sync.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.cioc.sync.service.ImportHistoricalDataService;

@RestController
@RequestMapping("/importHistoricalDataController")
public class ImportHistoricalDataController {

    @Autowired
    ImportHistoricalDataService importHistoricalDataService;

    @GetMapping("/importJSON")
    public void importJSON(@RequestParam String absolutePath, @RequestParam String collectionName,
            @RequestParam String sort) {
        importHistoricalDataService.importJsonFile(absolutePath, collectionName, sort);
    }
}
