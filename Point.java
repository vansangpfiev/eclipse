import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

public class Point {
	private Double x;
	private Double y;

	public Point(Double x, Double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public Double getX() {
		return x;
	}

	public void setX(Double x) {
		this.x = x;
	}

	public Double getY() {
		return y;
	}

	public void setY(Double y) {
		this.y = y;
	}

	public static Double getDistance(Point p1, Point p2) {
		return Math.sqrt(Math.pow(p1.getX() - p2.getX(), 2) + Math.pow(p1.getY() - p2.getY(), 2));
	}

	public static Double getMinDistance(Point p1, List<Point> list) {
		Double min = Double.MAX_VALUE;
		for (Point p : list) {
			if (getDistance(p1, p) < min)
				min = getDistance(p1, p);
		}
		return min / 2;
	}

	public static Point parsePoint(String s) {

		if (s.isEmpty())
			return new Point(0.0, 0.0);
		else {
			String[] xy = s.replaceAll("[\\( \\) \\s]", "").split(";");
			return new Point(Double.parseDouble(xy[0]), Double.parseDouble(xy[1]));
		}
	}

	public static List<Point> sortPoint(List<Point> list) {
		if (list.size() > 0) {
			Collections.sort(list, new Comparator<Point>() {
				@Override
				public int compare(final Point p1, final Point p2) {
					return p1.getX().compareTo(p2.getX());
				}
			});
		}
		return list;
	}

	public static Point generatePoint(Point p, Double radius) {
		Random r = new Random();
		Double x = p.getX() - radius + 2 * radius * r.nextDouble();
		Double y = p.getY() - Math.sqrt(Math.pow(radius, 2) - Math.pow(x - p.getX(), 2))
				+ 2 * Math.sqrt(Math.pow(radius, 2) - Math.pow(x - p.getX(), 2)) * r.nextDouble();

		return new Point((double) (Math.round(x * 1000)) / 1000, (double) (Math.round(y * 1000)) / 1000);
	}

	public static String printPoint(Point p) {
		return " (" + p.getX() + ";" + p.getY() + ") ";
	};
}
