package com.example.sonnh.controllers

import com.example.sonnh.services.CsvService
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.multipart.MultipartFile

@RestController
@RequestMapping("/upload")
class UploadFileController {

    @Autowired
    private lateinit var csvService: CsvService

    @PostMapping("/csv")
    fun uploadCsv(
        @RequestParam("file") multipartfile: MultipartFile,
        @RequestParam("keys") keysStr: String
    ): String {
        val keys: Collection<String> = jacksonObjectMapper().readValue(keysStr)
        csvService.uploadCsvFile(multipartfile, keys)
        return "kotlin"
    }
}