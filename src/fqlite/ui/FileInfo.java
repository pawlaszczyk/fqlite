package fqlite.ui;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.layout.*;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import fqlite.base.Global;

public class FileInfo {

	// ── Theme definitions ─────────────────────────────────────────────────────

	private static final class Theme {
		final String bgPage, bgCard, bgHeader, bgChip, bgCell;
		final String fgPrimary, fgMuted, fgHint, fgBadge;
		final String border;

		Theme(String bgPage, String bgCard, String bgHeader, String bgChip, String bgCell,
			  String fgPrimary, String fgMuted, String fgHint, String fgBadge, String border) {
			this.bgPage    = bgPage;
			this.bgCard    = bgCard;
			this.bgHeader  = bgHeader;
			this.bgChip    = bgChip;
			this.bgCell    = bgCell;
			this.fgPrimary = fgPrimary;
			this.fgMuted   = fgMuted;
			this.fgHint    = fgHint;
			this.fgBadge   = fgBadge;
			this.border    = border;
		}
	}

	private static final Theme LIGHT = new Theme(
			"#F5F4F0",  // bgPage
			"#FFFFFF",  // bgCard
			"#F1EFE8",  // bgHeader
			"#E6F1FB",  // bgChip
			"#F5F4F0",  // bgCell
			"#1A1A18",  // fgPrimary
			"#5F5E5A",  // fgMuted
			"#888780",  // fgHint
			"#185FA5",  // fgBadge
			"#D3D1C7"   // border
	);

	private static final Theme DARK = new Theme(
			"#1E1E1E",  // bgPage
			"#2A2A2A",  // bgCard
			"#252525",  // bgHeader
			"#1A3050",  // bgChip
			"#333333",  // bgCell
			"#E8E8E6",  // fgPrimary
			"#A0A09C",  // fgMuted
			"#6A6A66",  // fgHint
			"#5BA3E0",  // fgBadge
			"#3A3A3A"   // border
	);

	// ── State ─────────────────────────────────────────────────────────────────

	private Theme theme = LIGHT;

	/** Cached references for live theme switching (populated in getPanel()) */
	private ScrollPane cachedScroll;
	private VBox       cachedRoot;
	private VBox       cachedContent;

	private String sha256hash = "";
	private String md5hash    = "";
	private String sha1       = "";
	private String path;
	public  Path   file;

	private static final double FONT_SCALE = 1.25;

	// ── Constructor ───────────────────────────────────────────────────────────

	public FileInfo(String path) {
		this.path = path;
		computeHashes(path);
		file = Paths.get(path);
	}

	// ── Theme API ─────────────────────────────────────────────────────────────

	/**
	 * Switches between light and dark theme.
	 * If a panel has already been built via {@link #getPanel()}, the UI is
	 * updated immediately without rebuilding the whole scene graph.
	 *
	 * @param dark {@code true} → dark theme, {@code false} → light theme
	 */
	public void setTheme(boolean dark) {
		theme = dark ? DARK : LIGHT;
		if (cachedScroll != null) {
			rebuildPanel();
		}
	}

	/**
	 * Toggles between light and dark theme.
	 * Convenience wrapper around {@link #setTheme(boolean)}.
	 */
	public void toggleTheme() {
		setTheme(theme == LIGHT);
	}

	/** Returns {@code true} if the dark theme is currently active. */
	public boolean isDarkTheme() {
		return theme == DARK;
	}

	// ── Panel ─────────────────────────────────────────────────────────────────

	/**
	 * Builds and returns a JavaFX panel with all file metadata.
	 * The panel can be embedded in any container (BorderPane, TabPane, SplitPane …).
	 * Subsequent calls to {@link #setTheme(boolean)} will update the panel in-place.
	 *
	 * @return ScrollPane containing the full report
	 */
	public ScrollPane getPanel() {
		cachedScroll = buildScrollPane();
		return cachedScroll;
	}

	/** Rebuilds the panel content in-place whenever the theme changes. */
	private void rebuildPanel() {
		cachedScroll.setStyle(
				"-fx-background-color: " + theme.bgPage + ";" +
				"-fx-border-color: transparent;"
		);

		cachedRoot.getChildren().clear();
		cachedContent.getChildren().clear();

		Path p = Paths.get(path);
		Map<String, Object> attr;
		try {
			attr = Files.readAttributes(p, "*", LinkOption.NOFOLLOW_LINKS);
		} catch (IOException e) {
			return; // keep stale content on error
		}

		String now = LocalDateTime.now()
				.format(DateTimeFormatter.ofPattern("yyyy-MM-dd  HH:mm:ss"));

		cachedRoot.setStyle("-fx-background-color: " + theme.bgPage + ";");
		cachedRoot.getChildren().add(buildHeader(now));

		cachedContent.getChildren().addAll(
				buildFileSection(p, (Long) attr.get("size")),
				buildTimestampsSection(attr),
				buildHashesSection()
		);

		cachedRoot.getChildren().add(cachedContent);
		cachedRoot.getChildren().add(buildFooter());
	}

	private ScrollPane buildScrollPane() {
		Path p = Paths.get(path);
		Map<String, Object> attr;
		try {
			attr = Files.readAttributes(p, "*", LinkOption.NOFOLLOW_LINKS);
		} catch (IOException e) {
			e.printStackTrace();
			return errorPanel(e.getMessage());
		}

		String now = LocalDateTime.now()
				.format(DateTimeFormatter.ofPattern("yyyy-MM-dd  HH:mm:ss"));

		// ── Outer layout ──────────────────────────────────────────────────────
		cachedRoot = new VBox(0);
		cachedRoot.setStyle("-fx-background-color: " + theme.bgPage + ";");
		cachedRoot.getChildren().add(buildHeader(now));

		cachedContent = new VBox(10);
		cachedContent.setPadding(new Insets(14));
		cachedContent.getChildren().addAll(
				buildFileSection(p, (Long) attr.get("size")),
				buildTimestampsSection(attr),
				buildHashesSection()
		);

		cachedRoot.getChildren().add(cachedContent);
		cachedRoot.getChildren().add(buildFooter());

		// ── ScrollPane wrapper ────────────────────────────────────────────────
		ScrollPane scroll = new ScrollPane(cachedRoot);
		scroll.setFitToWidth(true);
		scroll.setHbarPolicy(ScrollPane.ScrollBarPolicy.NEVER);
		scroll.setStyle(
				"-fx-background-color: " + theme.bgPage + ";" +
				"-fx-border-color: transparent;"
		);
		return scroll;
	}

	// ── Header ────────────────────────────────────────────────────────────────
	private HBox buildHeader(String now) {
		HBox header = new HBox();
		header.setAlignment(Pos.CENTER_LEFT);
		header.setPadding(new Insets(12, 16, 12, 16));
		header.setStyle(
				"-fx-background-color: " + theme.bgHeader + ";" +
				"-fx-border-color: " + theme.border + ";" +
				"-fx-border-width: 0 0 1 0;"
		);

		VBox left = new VBox(3);
		left.getChildren().addAll(
				label("FQLite Forensic Report", 16, theme.fgPrimary, true),
				label("v" + Global.FQLITE_VERSION + "  ·  "
					  + System.getProperty("user.name") + "  ·  "
					  + System.getProperty("os.name"), 11, theme.fgHint, false)
		);

		Region spacer = new Region();
		HBox.setHgrow(spacer, Priority.ALWAYS);

		Label badge = label(now, 10, theme.fgBadge, false);
		badge.setPadding(new Insets(3, 10, 3, 10));
		badge.setStyle(
				"-fx-background-color: " + theme.bgChip + ";" +
				"-fx-background-radius: 99;" +
				"-fx-text-fill: " + theme.fgBadge + ";"
		);

		header.getChildren().addAll(left, spacer, badge);
		return header;
	}

	// ── File section ──────────────────────────────────────────────────────────
	private VBox buildFileSection(Path p, long size) {
		VBox card = card();

		Label pathLabel = label(path, 11, theme.fgMuted, false);
		pathLabel.setStyle("-fx-font-family: monospace; -fx-text-fill: " + theme.fgMuted + ";");
		pathLabel.setWrapText(true);

		Label sizeChip = label(formatSize(size), 11, theme.fgMuted, false);
		sizeChip.setPadding(new Insets(2, 8, 2, 8));
		sizeChip.setStyle(
				"-fx-background-color: " + theme.bgCell + ";" +
				"-fx-background-radius: 4;" +
				"-fx-border-color: " + theme.border + ";" +
				"-fx-border-radius: 4;" +
				"-fx-text-fill: " + theme.fgMuted + ";"
		);

		card.getChildren().addAll(
				sectionTitle("File"),
				label(p.getFileName().toString(), 14, theme.fgPrimary, true),
				pathLabel,
				sizeChip
		);
		return card;
	}

	// ── Timestamps section ────────────────────────────────────────────────────
	private VBox buildTimestampsSection(Map<String, Object> attr) {
		VBox card = card();

		HBox grid = new HBox(8);
		grid.getChildren().addAll(
				tsCard("Created",       String.valueOf(attr.get("creationTime"))),
				tsCard("Last accessed", String.valueOf(attr.get("lastAccessTime"))),
				tsCard("Last modified", String.valueOf(attr.get("lastModifiedTime")))
		);

		card.getChildren().addAll(sectionTitle("Timestamps"), grid);
		return card;
	}

	private VBox tsCard(String labelText, String value) {
		VBox box = new VBox(4);
		box.setPadding(new Insets(8, 10, 8, 10));
		box.setStyle(
				"-fx-background-color: " + theme.bgCell + ";" +
				"-fx-background-radius: 6;" +
				"-fx-border-color: " + theme.border + ";" +
				"-fx-border-radius: 6;"
		);
		HBox.setHgrow(box, Priority.ALWAYS);

		Label val = label(value.replaceAll("\\[.*\\]", "").trim(), 10, theme.fgPrimary, false);
		val.setStyle("-fx-font-family: monospace; -fx-text-fill: " + theme.fgPrimary + ";");
		val.setWrapText(true);

		box.getChildren().addAll(
				label(labelText.toUpperCase(), 9, theme.fgHint, false),
				val
		);
		return box;
	}

	// ── Hashes section ────────────────────────────────────────────────────────
	private VBox buildHashesSection() {
		VBox card = card();
		card.getChildren().add(sectionTitle("Cryptographic Hashes"));
		card.getChildren().add(hashRow("MD5",     md5hash,    "32 chars"));
		card.getChildren().add(separator());
		card.getChildren().add(hashRow("SHA-1",   sha1,       "40 chars"));
		card.getChildren().add(separator());
		card.getChildren().add(hashRow("SHA-256", sha256hash, "64 chars"));
		return card;
	}

	private HBox hashRow(String algo, String hash, String hint) {
		HBox row = new HBox(10);
		row.setAlignment(Pos.CENTER_LEFT);

		Label algoLbl = label(algo, 10, theme.fgMuted, true);
		algoLbl.setMinWidth(58);

		Label hashLbl = label(hash, 10, theme.fgPrimary, false);
		hashLbl.setStyle("-fx-font-family: monospace; -fx-text-fill: " + theme.fgPrimary + ";");
		hashLbl.setWrapText(true);
		HBox.setHgrow(hashLbl, Priority.ALWAYS);

		Label hintLbl = label(hint, 9, theme.fgHint, false);
		hintLbl.setMinWidth(52);
		hintLbl.setAlignment(Pos.CENTER_RIGHT);

		row.getChildren().addAll(algoLbl, hashLbl, hintLbl);
		return row;
	}

	// ── Footer ────────────────────────────────────────────────────────────────
	private HBox buildFooter() {
		HBox footer = new HBox();
		footer.setPadding(new Insets(7, 16, 7, 16));
		footer.setAlignment(Pos.CENTER_LEFT);
		footer.setStyle(
				"-fx-border-color: " + theme.border + ";" +
				"-fx-border-width: 1 0 0 0;" +
				"-fx-background-color: " + theme.bgHeader + ";"
		);

		Region sp = new Region();
		HBox.setHgrow(sp, Priority.ALWAYS);

		Label right = label("v" + Global.FQLITE_VERSION, 10, theme.fgHint, false);
		right.setStyle("-fx-font-family: monospace; -fx-text-fill: " + theme.fgHint + ";");

		footer.getChildren().addAll(
				label("FQLite Forensic Report · on-disk metadata", 10, theme.fgHint, false),
				sp,
				right
		);
		return footer;
	}

	// ── Error fallback ────────────────────────────────────────────────────────
	private ScrollPane errorPanel(String message) {
		Label err = label("Could not read file: " + message, 12, "#A32D2D", false);
		err.setPadding(new Insets(16));
		err.setWrapText(true);
		ScrollPane sp = new ScrollPane(err);
		sp.setFitToWidth(true);
		return sp;
	}

	// ── Helpers ───────────────────────────────────────────────────────────────
	private VBox card() {
		VBox box = new VBox(8);
		box.setPadding(new Insets(12, 14, 12, 14));
		box.setStyle(
				"-fx-background-color: " + theme.bgCard + ";" +
				"-fx-background-radius: 10;" +
				"-fx-border-color: " + theme.border + ";" +
				"-fx-border-radius: 10;"
		);
		return box;
	}

	private Label sectionTitle(String text) {
		Label lbl = label(text.toUpperCase(), 9, theme.fgHint, false);
		lbl.setPadding(new Insets(0, 0, 2, 0));
		return lbl;
	}

	private Region separator() {
		Region r = new Region();
		r.setPrefHeight(1);
		r.setStyle("-fx-background-color: " + theme.border + ";");
		return r;
	}

	private Label label(String text, int size, String color, boolean bold) {
		Label lbl = new Label(text);
		lbl.setFont(bold
				? Font.font("System", FontWeight.MEDIUM, size * FONT_SCALE)
				: Font.font("System", size * FONT_SCALE));
		lbl.setTextFill(Color.web(color));
		return lbl;
	}

	private String formatSize(long bytes) {
		if (bytes < 1_024)
			return bytes + " B";
		if (bytes < 1_048_576)
			return String.format("%.1f KB  (%,d bytes)", bytes / 1_024.0, bytes);
		if (bytes < 1_073_741_824)
			return String.format("%.2f MB  (%,d bytes)", bytes / 1_048_576.0, bytes);
		return String.format("%.2f GB  (%,d bytes)", bytes / 1_073_741_824.0, bytes);
	}

	public void computeHashes(String path) {
		try {
			sha256hash = new DigestUtils(
					org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_256)
					.digestAsHex(new File(path));
			md5hash = new DigestUtils(
					org.apache.commons.codec.digest.MessageDigestAlgorithms.MD5)
					.digestAsHex(new File(path));
			sha1 = new DigestUtils(
					org.apache.commons.codec.digest.MessageDigestAlgorithms.SHA_1)
					.digestAsHex(new File(path));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// ── Legacy API ────────────────────────────────────────────────────────────
	@Override
	public String toString() {
		return "FileInfo[" + path + "]";
	}
}
