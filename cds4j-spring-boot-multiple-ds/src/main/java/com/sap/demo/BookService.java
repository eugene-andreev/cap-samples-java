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

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class BookService {

    private final CdsDataStore dataStore;

    private final AuthorService authorService;

    public BookService(CdsDataStoreConnector primaryConnector, AuthorService authorService) {
        this.dataStore = primaryConnector.connect();
        this.authorService = authorService;
    }

    @Transactional
    public void saveBooks(List<Map<String, Object>> books) {
        CqnInsert insert = Insert.into("Book").entries(books);
        dataStore.execute(insert);
    }

    @Transactional
    public Result readAllBooks() {
        CqnSelect select = Select.from("Book");
        return dataStore.execute(select);
    }

    @Transactional(propagation = Propagation.NESTED)
    public void saveBooksWithAuthors(List<Map<String, Object>> books, List<Map<String, Object>> authors) {
        saveBooks(books);
        authorService.saveAuthors(authors); // Suspend current transaction and create an inner `saveAuthors` transaction
    }

    @Transactional
    public void deleteAllBooks() {
        dataStore.execute(Delete.from("Book"));
    }
}
