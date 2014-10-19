package tree;

import tree.impl.PageServiceImpl;
import tree.impl.TreeException;

/**
 *
 */
public class PageServiceFactory {

    public static PageService create() throws TreeException {
        return PageServiceImpl.getInstance();
    }

}
