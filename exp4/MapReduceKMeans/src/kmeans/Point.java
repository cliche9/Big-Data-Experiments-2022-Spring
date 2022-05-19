package kmeans;

public class Point {
    int _x;
    int _y;
    int _z;

    public Point() {
        _x = 0;
        _y = 0;
        _z = 0;
    }

    public Point(String pointInfo) {
        String[] list = pointInfo.split(" ");
        _x = Integer.parseInt(list[0]);
        _y = Integer.parseInt(list[1]);
        _z = Integer.parseInt(list[2]);
    }

    public Point(Point p) {
        _x = p._x;
        _y = p._y;
        _z = p._z;
    }

    @Override
    public String toString() {
        return Integer.toString(_x) + ' ' + Integer.toString(_y) + ' ' + Integer.toString(_z) + ' ';
    }

    public Point add(Point p) {
        p._x += this._x;
        p._y += this._y;
        p._z += this._z;
        return p;
    }

    public Point sub(Point p) {
        Point res = new Point(this);
        res._x -= p._x;
        res._y -= p._y;
        res._z -= p._z;
        return res;
    }

    public Point divide(int n) {
        this._x /= n;
        this._y /= n;
        this._z /= n;
        return this;
    }

    public double norm() {
        return Math.sqrt(Math.pow(this._x, 2) + Math.pow(this._y, 2) + Math.pow(this._z, 2));
    }
}
