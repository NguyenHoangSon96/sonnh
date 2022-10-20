package com.example.sonnh.repositories

import com.example.sonnh.services.RowObject
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Repository
import javax.persistence.EntityManager

@Repository
class CsvRepository {

    @Autowired
    private lateinit var entityManager: EntityManager

    fun createDynamicTable(tableName: String, keys: Set<String>, columns: Set<String>) {
        val query = StringBuilder()
        query.append("DROP TABLE IF EXISTS \"$tableName\";")
        query.append("Create table \"${tableName}\"")
        query.append(" (")
        query.append(" id serial PRIMARY KEY,")
        for (column in columns) {
            query.append(" $column Text, ")
        }
        query.append(" created_date timestamp,")
        query.append(" updated_date timestamp")
        query.append(" );")

        entityManager.createNativeQuery(query.toString()).executeUpdate()
    }

    fun upsertRow(tableName: String, row: RowObject, keys: Set<String>) {

//        if (keys.isEmpty()) {
//            return
//        }
//
//        if (isRowExists(tableName, row, keys)) {
//
//        } else {
//
//        }
    }

    private fun isRowExists(tableName: String, row: RowObject, tableKeys: Set<String>): Boolean {
        var result = false
//        var query = StringBuilder()
//        query.append("SELECT count(id) FROM \"$tableName\"")
//        query.append(" WHERE")
//        tableKeys.forEach {
//            query.append("$it = ${row.fin}")
//        }
//        query.append(" ")
//        query.deleteCharAt(query.lastIndexOf(","))

        return result
    }
}