package com.sap.demo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sap.cds.CdsException;
import com.sap.cds.Result;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;
import org.springframework.test.context.jdbc.SqlConfig;
import org.springframework.test.context.jdbc.SqlConfig.TransactionMode;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@Sql(scripts = {
        "/dbPrimary/schema.sql" }, config = @SqlConfig(encoding = "utf-8", transactionMode = TransactionMode.ISOLATED))
@Sql(scripts = {
        "/dbSecondary/schema.sql" }, config = @SqlConfig(encoding = "utf-8", dataSource = "secondaryDS", transactionManager = "secondaryTx", transactionMode = TransactionMode.ISOLATED))
public class IntegrationTest {

    @Autowired
    private BookService bookService;

    @Autowired
    private AuthorService authorService;

    private List<Map<String, Object>> booksData;
    private List<Map<String, Object>> authorsData;
    private String expectedBooksJson;
    private String expectedAuthorsJson;

    private static final ObjectMapper JSONIZER = new ObjectMapper();

    @Before
    public void init() throws JsonProcessingException {
        booksData = DataProvider.books();
        authorsData = DataProvider.authors();

        expectedBooksJson = JSONIZER.writeValueAsString(booksData);
        expectedAuthorsJson = JSONIZER.writeValueAsString(authorsData);
    }

    /**
     * Demonstrates sequential Transaction processing.
     * 
     * start TX1
     *   SQL >> INSERT INTO Book
     * end TX1
     * start TX2
     *   SQL >> INSERT INTO Author
     * end TX2
     */
    @Test
    public void testWriteReadDataFrom2DataSources() {
        bookService.saveBooks(booksData);
        authorService.saveAuthors(authorsData);

        Result allBooks = bookService.readAllBooks();
        Result allAuthors = authorService.readAllAuthors();

        assertThat(allBooks.toJson()).isEqualTo(expectedBooksJson);
        assertThat(allAuthors.toJson()).isEqualTo(expectedAuthorsJson);
    }

    /**
     * Demonstrates inner Transaction processing.
     * 
     * start TX1
     *   SQL >> INSERT INTO Book
     *   suspend TX1
     *   start TX2
     *     SQL >> INSERT INTO Author
     *   end TX2
     *   resume TX1
     * end TX1
     */
    @Test
    public void testWriteBooksAndAuthors() {
        bookService.saveBooksWithAuthors(booksData, authorsData);

        Result allBooks = bookService.readAllBooks();
        Result allAuthors = authorService.readAllAuthors();

        assertThat(allBooks.toJson()).isEqualTo(expectedBooksJson);
        assertThat(allAuthors.toJson()).isEqualTo(expectedAuthorsJson);
    }

    @Test
    public void testWriteDataToWrongDataSource() {
        Assertions.assertThatExceptionOfType(CdsException.class).isThrownBy(() -> authorService.saveAuthors(booksData))
                .withMessage("Element 'title' does not exist in entity 'Author'");
    }

    @After
    public void cleanup() {
        bookService.deleteAllBooks();
        authorService.deleteAllAuthors();
    }
}
