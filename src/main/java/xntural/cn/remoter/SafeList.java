package xntural.cn.remoter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;


/**
 * 安全列表
 * @param <E>
 */
class SafeList <E> {
    protected final ArrayList<E>  data = new ArrayList<>();
    final           ReadWriteLock lock = new ReentrantReadWriteLock();

    E findAny(Function<E, Boolean> fn) {
        try {
            lock.readLock().lock();
            for (E e : data) {
                if (fn.apply(e)) return e;
            }
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    /**
     * 随机取一个 元素
     * @param predicate 符合条件的元素
     * @return
     */
    E findRandom(Predicate<E> predicate) {
        try {
            lock.readLock().lock();
            if (data.isEmpty()) return null;
            if (predicate == null) {
                return data.get(new Random().nextInt(data.size()));
            } else {
                List<E> ls = data.stream().filter(predicate).collect(Collectors.toList());
                if (ls.isEmpty()) return null;
                return ls.get(new Random().nextInt(ls.size()));
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    void withWriteLock(Runnable fn) {
        if (fn == null) return;
        try {
            lock.writeLock().lock();
            fn.run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    void withReadLock(Runnable fn) {
        if (fn == null) return;
        try {
            lock.readLock().lock();
            fn.run();
        } finally {
            lock.readLock().unlock();
        }
    }

    Iterator<E> iterator() { return data.iterator(); }

    int size() { return data.size(); }

    boolean isEmpty() { return data.isEmpty(); }

    boolean contains(Object o) {
        try {
            lock.readLock().lock();
            return data.contains(o);
        } finally {
            lock.readLock().unlock();
        }
    }

    boolean remove(Object o) {
        try {
            lock.writeLock().lock();
            return data.remove(o);
        } finally {
            lock.writeLock().unlock();
        }
    }

    boolean add(E e) {
        try {
            lock.writeLock().lock();
            return data.add(e);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
