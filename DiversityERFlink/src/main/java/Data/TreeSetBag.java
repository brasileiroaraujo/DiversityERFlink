package Data;

import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

public class TreeSetBag<E> extends TreeSet<E> {

    private final int limit;

    public TreeSetBag(final int limit) {
        super();
        this.limit = limit;
    }

    public TreeSetBag(final int limit, final Collection<? extends E> c) {
        super(c);
        this.limit = limit;
    }

    public TreeSetBag(final int limit, final Comparator<? super E> comparator) {
        super(comparator);
        this.limit = limit;
    }

    public TreeSetBag(final int limit, final SortedSet<E> s) {
        super(s);
        this.limit = limit;
    }

    @Override
    public boolean add(final E e) {
        if (size() >= limit) {
        	super.add(e);
        	E removed = super.pollLast();
            return !e.equals(removed); //it means that e was not added.
        }

        return super.add(e);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (size() + c.size() >= limit) {
            return false;
        }

        return super.addAll(c);
    }
}
