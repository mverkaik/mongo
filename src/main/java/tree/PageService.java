package tree;

import tree.impl.PageImpl;

import java.util.List;

/**
 *
 */
public interface PageService {

    public void save(Page page);

    public Page create(String title, String description);

    public Page create(String title, String description, Page parent);

    public Page get(String id);

    public List<Page> getChildren(Page parent);

    public void delete(String id);

    public void deleteAll();

    public long count();

}
