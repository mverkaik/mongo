package tree;

import org.junit.Before;
import org.junit.Test;
import tree.impl.TreeException;

import static org.junit.Assert.*;

/**
 *
 */
public class TreeUnitTest {

    private PageService pageService;

    private String BOOKS = "Books", PROG = "Programming", LANG = "Languages", DBS = "Databases", MONGO = "MongoDB",
            DBM = "dbm";

    public TreeUnitTest() throws TreeException {
        pageService = PageServiceFactory.create();
    }

    @Before
    public void beforeTest() {
        pageService.deleteAll();
    }

    @Test
    public void pageCreationAndChildrenTest() {

        Page books = pageService.create(BOOKS, BOOKS);
        Page programming = pageService.create(PROG, PROG, books);
        Page languages = pageService.create(LANG, LANG, programming);
        Page databases = pageService.create(DBS, DBS, programming);
        pageService.create(MONGO, MONGO, databases);
        pageService.create(DBM, DBM, databases);

        assertEquals(PROG, pageService.getChildren(books).get(0).getTitle());
        assertEquals(DBS, pageService.getChildren(programming).get(0).getTitle());
        assertEquals(LANG, pageService.getChildren(programming).get(1).getTitle());
        assertEquals(MONGO, pageService.getChildren(databases).get(0).getTitle());
        assertEquals(DBM, pageService.getChildren(databases).get(1).getTitle());
        assertEquals(0, pageService.getChildren(languages).size());

    }

    @Test
    public void pageGetAndDeletionTest() {

        assertEquals(0, pageService.count());
        Page books = pageService.create(BOOKS, BOOKS);
        assertEquals(1, pageService.count());
        assertEquals(BOOKS, pageService.get(books.getId()).getTitle());
        pageService.delete(books.getId());
        assertNull(pageService.get(books.getId()));
        assertEquals(0, pageService.count());

    }


}
