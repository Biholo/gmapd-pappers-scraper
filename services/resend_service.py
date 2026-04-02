import os
import resend


class ResendService:
    def __init__(self, api_key: str = None, from_email: str = None):
        self.api_key = api_key or os.getenv("RESEND_API_KEY")
        if not self.api_key:
            raise ValueError("RESEND_API_KEY must be provided")
        resend.api_key = self.api_key
        self.from_email = from_email or os.getenv("RESEND_FROM_EMAIL", "SCI Scraper <onboarding@resend.dev>")

    def send_report(self, to: list[str], subject: str, html_body: str) -> dict:
        params: resend.Emails.SendParams = {
            "from": self.from_email,
            "to": to,
            "subject": subject,
            "html": html_body,
        }
        return resend.Emails.send(params)

    def send_scraping_report(
        self,
        to: list[str],
        date_scraped: str,
        total_sci: int,
        days_scraped: list[dict],
        duration_seconds: float = 0,
    ) -> dict:
        days_rows = ""
        for day in days_scraped:
            days_rows += f"""
            <tr>
                <td style="padding: 8px 12px; border: 1px solid #ddd;">{day['date']}</td>
                <td style="padding: 8px 12px; border: 1px solid #ddd; text-align: center;">{day['count']}</td>
            </tr>"""

        duration_min = duration_seconds / 60 if duration_seconds else 0

        html = f"""
        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
            <h2 style="color: #2c3e50;">Rapport de scraping SCI</h2>
            <p>Date d'exécution : <strong>{date_scraped}</strong></p>
            <p>Durée : <strong>{duration_min:.1f} minutes</strong></p>

            <h3 style="color: #34495e;">Résumé</h3>
            <p style="font-size: 24px; color: #27ae60; font-weight: bold;">{total_sci} SCI scrappées au total</p>

            <h3 style="color: #34495e;">Détail par jour</h3>
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr style="background-color: #ecf0f1;">
                        <th style="padding: 8px 12px; border: 1px solid #ddd; text-align: left;">Date de publication</th>
                        <th style="padding: 8px 12px; border: 1px solid #ddd; text-align: center;">Nombre de SCI</th>
                    </tr>
                </thead>
                <tbody>
                    {days_rows}
                </tbody>
            </table>

            <p style="color: #7f8c8d; font-size: 12px; margin-top: 20px;">
                Ce rapport a été généré automatiquement par le scraper SCI.
            </p>
        </div>
        """

        return self.send_report(
            to=to,
            subject=f"Rapport SCI - {total_sci} entreprises scrappées le {date_scraped}",
            html_body=html,
        )
