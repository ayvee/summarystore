package com.samsung.sra.DataStore;

/**
 * Created by n.agrawal1 on 5/31/16.
 */
public class ResultError<R, E> {
    public final R result;
    public final E error;

    public ResultError(R result, E error) {
        this.result = result;
        this.error = error;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ResultError<?, ?> that = (ResultError<?, ?>) o;

        if (result != null ? !result.equals(that.result) : that.result != null) return false;
        return error != null ? error.equals(that.error) : that.error == null;

    }

    @Override
    public int hashCode() {
        int result = this.result != null ? this.result.hashCode() : 0;
        result = 31 * result + (error != null ? error.hashCode() : 0);
        return result;
    }
}
