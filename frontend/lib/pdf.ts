import html2canvas from "html2canvas";
import jsPDF from "jspdf";

export interface PDFOptions {
  filename: string;
  title?: string;
  subtitle?: string;
}

/**
 * Generate a PDF from an HTML element (client-side)
 * Optimized for smaller file sizes and better rendering
 */
export async function generatePDFFromElement(
  element: HTMLElement,
  options: PDFOptions
): Promise<void> {
  const { filename, title, subtitle } = options;

  // Create canvas with optimized settings
  const canvas = await html2canvas(element, {
    scale: 1.5, // Reduced from 2 for smaller file size
    useCORS: true,
    logging: false,
    backgroundColor: "#ffffff",
    windowWidth: 1600, // Wider width to prevent text truncation in charts
  });

  // Use JPEG with compression instead of PNG
  const imgData = canvas.toDataURL("image/jpeg", 0.8);

  // Use A4 portrait for standard printing
  const pdf = new jsPDF({
    orientation: "portrait",
    unit: "mm",
    format: "a4",
  });

  const pageWidth = pdf.internal.pageSize.getWidth();
  const pageHeight = pdf.internal.pageSize.getHeight();
  const margin = 10;
  const headerHeight = 25;
  const footerHeight = 10;
  const contentWidth = pageWidth - margin * 2;

  // Calculate image dimensions to fit page
  const imgAspectRatio = canvas.width / canvas.height;
  const availableHeight = pageHeight - headerHeight - footerHeight - margin * 2;
  let imgWidth = contentWidth;
  let imgHeight = imgWidth / imgAspectRatio;

  // If image is taller than available space, scale down
  if (imgHeight > availableHeight) {
    imgHeight = availableHeight;
    imgWidth = imgHeight * imgAspectRatio;
  }

  // Add header
  pdf.setFillColor(102, 126, 234);
  pdf.rect(0, 0, pageWidth, headerHeight, "F");

  pdf.setTextColor(255, 255, 255);
  pdf.setFontSize(14);
  pdf.setFont("helvetica", "bold");
  pdf.text(title || "Daily Report", margin, 10);

  if (subtitle) {
    pdf.setFontSize(9);
    pdf.setFont("helvetica", "normal");
    pdf.text(subtitle, margin, 17);
  }

  // Add the captured image centered
  const xOffset = (pageWidth - imgWidth) / 2;
  pdf.addImage(imgData, "JPEG", xOffset, headerHeight + 5, imgWidth, imgHeight);

  // Add footer
  pdf.setTextColor(128, 128, 128);
  pdf.setFontSize(8);
  pdf.setFont("helvetica", "normal");
  pdf.text(
    `Generated on ${new Date().toLocaleString()} • Paul Logistics Analytics`,
    margin,
    pageHeight - 5
  );

  // Save the PDF
  pdf.save(filename);
}

/**
 * Future: Server-side PDF generation endpoint
 */
export async function generatePDFFromAPI(
  reportDate: string,
  orgId?: string
): Promise<Blob> {
  // TODO: Implement when Option 2 is needed
  throw new Error("Server-side PDF generation not yet implemented");
}

/**
 * Download a blob as a file
 */
export function downloadBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}
