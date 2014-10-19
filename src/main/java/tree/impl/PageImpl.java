package tree.impl;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import tree.Page;

/**
 *
 */
@Entity("pages")
public class PageImpl implements Page {

    @Id
    private String id = new ObjectId().toString();

    private String title, description;

    private String path = null;

    PageImpl() {}

    PageImpl(String title, String description, Page parent) {
        this.title = title;
        this.description = description;
        if (parent != null) {
            String ppath = ((PageImpl) parent).getPath();
            String parentPath =
                    ppath == null ? "" : ppath.substring(0, ppath.length() - 1);
            path = String.format("%s,%s,", parentPath, parent.getId().toString());
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return "BasePage{" +
                "path='" + path + '\'' +
                ", id=" + id +
                ", title='" + title + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
