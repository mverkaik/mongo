package tree.impl;

import com.mongodb.MongoClient;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.Query;
import tree.Page;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 *
 */
public class PageServiceImpl implements tree.PageService {

    private static final Logger LOG = Logger.getLogger(PageServiceImpl.class.getName());

    private static PageServiceImpl pageService;

    private Datastore datastore;

    private PageServiceImpl() {}

    public static synchronized PageServiceImpl getInstance() throws TreeException {
        if (pageService == null) {
            pageService = new PageServiceImpl();
            try {
                Morphia morphia = new Morphia();
                morphia.map(PageImpl.class);
                Datastore datastore = morphia.createDatastore(new MongoClient(), "tree");
                datastore.ensureIndex(PageImpl.class, "path");
                pageService.setDataStore(datastore);
            } catch (UnknownHostException e) {
                throw new TreeException(e.getMessage());
            }
            LOG.info("Page service is initialized");
        }
        return pageService;
    }

    void setDataStore(Datastore dataStore) {
        this.datastore = dataStore;
    }

    public Page create(String title, String description) {
        return create(title, description, null);
    }

    public Page create(String title, String description, Page parent) {
        PageImpl page = new PageImpl(title, description, parent);
        save(page);
        LOG.fine("Created page: "+page);
        return page;
    }

    public void save(Page page) {
        datastore.save(page).getId();
    }

    public Page get(String id) {
        return datastore.get(PageImpl.class, id);
    }

    public List<Page> getChildren(Page parent) {
        String regex;
        String parentPath = ((PageImpl) parent).getPath();
        if (parentPath == null) {
            regex = String.format("^,%s,$", parent.getId().toString());
        } else {
            regex = String.format("^%s%s,$", parentPath, parent.getId().toString());
        }
        Pattern regexp = Pattern.compile(regex);
        Query<PageImpl> query = datastore.createQuery(PageImpl.class).filter("path", regexp).order("path").order("title");
        List children = query.asList();
        LOG.fine(String.format("Got %s children for page %s", children.size(), parent.toString()));
        return Collections.checkedList(children, Page.class);
    }

    public void delete(String id) {
        datastore.delete(PageImpl.class, id);
        LOG.fine(String.format("Deleted page with id %s", id));
    }

    public void deleteAll() {
        datastore.delete(datastore.createQuery(PageImpl.class));
        LOG.fine("All pages were deleted");
    }

    public long count() {
        long count = datastore.getCount(PageImpl.class);
        LOG.fine("There are %s pages in the store");
        return count;
    }
}
