package com.samsung.sra.DataStore;

/** TODO?: return confidence level along with error? */
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

    @Override
    public String toString() {
        return String.format("<%s, %s>", result, error);
    }
}
