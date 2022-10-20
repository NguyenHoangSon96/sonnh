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
        if (isRowExists(tableName, row, keys)) {

        } else {

        }
    }

    private fun isRowExists(tableName: String, row: RowObject, tableKeys: Set<String>): Boolean {
        val query = StringBuilder()
        query.append("SELECT count(id) FROM \"$tableName\"")
        query.append(" WHERE")
        tableKeys.forEachIndexed { index, key ->
            query.append(" $key = ${row.value}${if (index != (tableKeys.size - 1)) "," else ""}")
        }
        val result: Int = entityManager.createNativeQuery(query.toString()).singleResult as Int
        return result > 0
    }
}