FROM python:3.9
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt
ENV PORT=5000
EXPOSE 5000
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "--timeout", "1200", "application:app"]

ENV PYTHONUNBUFFERED=1
ENV GLOBO_DIR_ITENS "globo-files/itens"
ENV GLOBO_DIR_TREINO "globo-files/treino"
ENV FILE_HISTORY_CATEGORY "file_history_category.csv"
ENV FILE_HISTORY_RANKING "file_history_ranking.csv"
ENV FILE_USER_X_HISTORY "file_user_x_history.csv"
ENV FILE_USER_X_PREFERENCES "file_user_x_preferences.csv"
ENV FILE_HISTORY_CATEGORY_E_DATA "file_history_category_e_data.csv"