package com.example.sonnh.services

import com.example.sonnh.repositories.CsvRepository
import lombok.extern.log4j.Log4j
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.multipart.MultipartFile
import java.nio.charset.Charset
import javax.transaction.Transactional
import kotlin.reflect.jvm.internal.impl.descriptors.Visibilities.Public

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
        val filePath = FileUtils.getFile("src/main/resources/temp/$name")
        FileUtils.writeByteArrayToFile(filePath, file.bytes)

        val contents = FileUtils.readLines(filePath, Charset.defaultCharset())
        val firstLineList = contents[0].split(",")
        val columns = firstLineList.toSet()
        if (firstLineList.size != columns.size) {
            throw Exception("Column is duplicated")
        }

        val tableName = name.replace(".csv", "").lowercase().replace(" ", "_")
        val tableKeys = keys.map { it.lowercase().replace(" ", "_") }.toSet()
        val tableColumns = columns.map {
            it.lowercase()
                .replace(" ", "_")
                .replace("?", "")
                .replace("(", "")
                .replace(")", "")
        }.toSet()
        csvRepository.createDynamicTable(tableName, tableKeys.toSet(), tableColumns.toSet())

        // row to map
        val rows = mutableListOf<RowObject>()
        for ((i, line) in contents.withIndex()) {
            if (i == 0) continue
            val values = line.split(",")
            for ((j, columnDisplayName) in columns.withIndex()) {
                val columnName = columnDisplayName.lowercase().replace(" ", "_")
                val value = values[j]
                rows.add(RowObject(columnName, columnDisplayName, value));
            }
        }
        log.info(rows.toString())

        for ((index, row) in rows.withIndex()) {
            csvRepository.upsertRow(tableName, row, tableKeys)
        }


    }

}

data class RowObject(var columnName: String, var columnDisplayName: String, var value: String)