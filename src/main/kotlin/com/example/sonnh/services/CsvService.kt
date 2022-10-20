package com.example.sonnh.services

import com.example.sonnh.repositories.CsvRepository
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.multipart.MultipartFile
import java.nio.charset.Charset
import javax.transaction.Transactional

@Service
@Transactional
class CsvService {

    @Autowired
    private lateinit var csvRepository: CsvRepository

    private val log = LoggerFactory.getLogger(this.javaClass)

    fun uploadCsvFile(file: MultipartFile, keys: Collection<String>) {
        val name = file.originalFilename
        if (name.isNullOrEmpty() || !name.endsWith("csv") || keys.isEmpty()) {
            return
        }
        //todo use file reader of kotlin
        val filePath = FileUtils.getFile("src/main/resources/temp/$name")
        FileUtils.writeByteArrayToFile(filePath, file.bytes)

        val contents = FileUtils.readLines(filePath, Charset.defaultCharset())
        val firstLineList = contents[0].replace("?", "").replace("(", "").replace(")", "").split(",")
        val columns = firstLineList.toSet()
        if (firstLineList.size != columns.size) {
            throw Exception("Column is duplicated")
        }

        val tableName = name.replace(".csv", "").lowercase().replace(" ", "_")
        val tableKeys = keys.map { it.lowercase().replace(" ", "_") }.toSet()
        val tableColumns = columns.map { it.lowercase().replace(" ", "_") }.toSet()
        csvRepository.createDynamicTable(tableName, tableKeys.toSet(), tableColumns.toSet())

        // row to map
        var rowMaps = mutableListOf<Map<String, Any>>()
        for ((i, line) in contents.withIndex()) {
            val rows = line.split(",")
            if (i == 0) continue
            val rowMap = mutableMapOf<String, Any>()
            tableColumns.forEachIndexed { index, tableColumn ->
                rowMap[tableColumn] = rows[index]
            }
            rowMaps.add(rowMap)
        }

        for (rowMap in rowMaps) {
            csvRepository.upsertRow(tableName, rowMap, tableKeys)
        }


    }

}