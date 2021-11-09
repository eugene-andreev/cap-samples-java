package com.sap.demo;

import java.util.List;
import java.util.Map;

import com.sap.cds.CdsDataStore;
import com.sap.cds.CdsDataStoreConnector;
import com.sap.cds.Result;
import com.sap.cds.ql.Delete;
import com.sap.cds.ql.Insert;
import com.sap.cds.ql.Select;
import com.sap.cds.ql.cqn.CqnInsert;
import com.sap.cds.ql.cqn.CqnSelect;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class AuthorService {

    private final CdsDataStore dataStore;

    public AuthorService(@Qualifier("secondary") CdsDataStoreConnector secondaryConnector) {
        dataStore = secondaryConnector.connect();
    }

    @Transactional(transactionManager = "secondaryTx")
    public void saveAuthors(List<Map<String, Object>> authors) {
        CqnInsert insert = Insert.into("Author").entries(authors);
        dataStore.execute(insert);
    }

    @Transactional(transactionManager = "secondaryTx")
    public Result readAllAuthors() {
        CqnSelect select = Select.from("Author");
        return dataStore.execute(select);
    }

    @Transactional(transactionManager = "secondaryTx")
    public void deleteAllAuthors() {
        dataStore.execute(Delete.from("Author"));
    }
}
