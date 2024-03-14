package com.cioc.sync.service;

public interface ImportHistoricalDataService {

    /**
     * 导入json文件
     * 
     * @param filePath       服务器上文件的绝对路径
     * @param collectionName
     * @param sort           导入时是按照文件正序还是倒叙导入 asc desc
     */
    void importJsonFile(String absolutePath, String collectionName, String sort);

}
